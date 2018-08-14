import threading

from django.db import models
from django import forms
from django.core.exceptions import ImproperlyConfigured
from django.contrib.admin.widgets import AdminIntegerFieldWidget
from django.db.models import F
from django.db.models.query_utils import DeferredAttribute

class optimisticLocking(object):
    """Enable optimistic locking - contextmanager or decorator.
        Can be applied to particular model instance, model class or globally per thread
    """
    current_state = threading.local()

    def __init__(self, model=None):
        self.model = model

    def __enter__(self):
        stateObj = self.model or self.current_state
        stateObj._optimisticLocking = getattr(stateObj, '_optimisticLocking', 0) + 1

    def __exit__(self, *args, **kwds):
        stateObj = self.model or self.current_state
        stateObj._optimisticLocking = getattr(stateObj, '_optimisticLocking', 1) - 1

    def __call__(self, func):
        def wrapper(*args, **kwds):
            with self:
                return func(*args, **kwds)
        return wrapper

    @classmethod
    def active(cls, model=None):
        return getattr(cls.current_state, '_optimisticLocking', 0) or (model and getattr(model, '_optimisticLocking', 0))


class ConcurrentUpdate(Exception):
    """
    Raised when a model can not be saved due to a concurrent update.
    """
    def __init__(self, model):
        model.refresh_from_db() # to return actual DB version, as `model` is tainted with deserialized values
        self.model = model


class ReadonlyInput(forms.TextInput):
    """
    A HiddenInput would be perfect for version fields, but hidden
    inputs leave ugly empty rows in the admin. The version must
    be submitted, of course, to be checked, so we can't just use
    ModelAdmin.readonly_fields.

    Pending Django ticket #11277, this displays the version in an
    uneditable input so there's no empty row in the admin table.

    https://code.djangoproject.com/ticket/11277
    """
    def __init__(self, *args, **kwargs):
        super(ReadonlyInput, self).__init__(*args, **kwargs)
        # just readonly, because disabled won't submit the value
        self.attrs['readonly'] = 'readonly'


class VersionField(models.PositiveIntegerField):
    """
    An integer field to track versions. Every time the model is saved,
    it is incremented by one.
    """

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('default', 0)
        super(VersionField, self).__init__(*args, **kwargs)

    def formfield(self, **kwargs):
        widget = kwargs.get('widget')
        if widget:
            if issubclass(widget, AdminIntegerFieldWidget):
                widget = ReadonlyInput()
        else:
            widget = forms.HiddenInput
        kwargs['widget'] = widget
        return super(VersionField, self).formfield(**kwargs)


class VersionedMixin(object):
    """
    Model mixin implementing version checking during saving.
    When a concurrent update is detected, saving is aborted and ConcurrentUpdate will be raised.

    Limitation: as it is hooked to `Model._do_update` it is not able to increment version or check locks for QuerySet.update calls.
    """

    def _do_insert(self, manager, using, fields, update_pk, raw):
        """Be defensive on inserts with version=None - just treat None as default"""
        version_field = self.get_version_field()
        client_version = version_field.value_from_object(self)
        if client_version is None:
            setattr(self, version_field.attname, version_field.default)
        return super(VersionedMixin, self)._do_insert(manager, using, fields, update_pk, raw)


    def _do_update(self, base_qs, using, pk_val, values, update_fields, forced_update):
        version_field = self.get_version_field()

        # _do_update is called once for each model in the inheritance
        # hierarchy. We only care about the model with the version field.
        if version_field.model != base_qs.model:
            return super(VersionedMixin, self)._do_update(
                base_qs, using, pk_val, values, update_fields, forced_update)

        if version_field.attname in self.get_deferred_fields():
            # With a deferred VersionField, it's not possible to do any
            # sensible concurrency checking, so throw an error. The
            # other option would be to treat deferring the VersionField
            # the same as excluding it from `update_fields` -- a way to
            # bypass checking altogether.
            raise RuntimeError("It doesn't make sense to save a model with a deferred VersionField")

        # pre_save may or may not have been called at this point, based on if
        # version_field is in update_fields. Since we need to reliably know the
        # old version, we can't increment there yet.

        # client version, e.g. provided by client through API call
        client_version = version_field.value_from_object(self)

        # if version provided, enable optimistic locking by adding WHERE version to UPDATE query
        version_qs = base_qs
        if client_version is not None and optimisticLocking.active(self):
            version_qs = version_qs.filter(**{version_field.attname: client_version})

        # so increment it here instead. Now old_version is reliable.
        for i, (field, model, value) in enumerate(values):
            if isinstance(field, VersionField):
                assert client_version is None or client_version == value
                # update version only if optimistic locking provided
                if value is not None:
                    new_version = value + 1
                    values[i] = (field, model, new_version)
                    setattr(self, version_field.attname, new_version)
                else:
                    # we don't want to redundantly load the object being edited to increment version when not provided
                    # F expression translates to "UPDATE table SET version = version + 1"
                    values[i] = (field, model, F(version_field.attname) + 1)

        updated = super(VersionedMixin, self)._do_update(
            base_qs=version_qs,
            using=using,
            pk_val=pk_val,
            values=values,
            update_fields=update_fields if values else None,  # Make sure base_qs is always checked
            forced_update=forced_update
        )

        if not updated and base_qs.filter(pk=pk_val).exists():
            raise ConcurrentUpdate(self)

        return updated

    def get_version_field(self):
        for field in self._meta.fields:
            if isinstance(field, VersionField):
                return field
        raise ImproperlyConfigured(
            'VersionedMixin models must have a VersionField')


class VersionedModel(VersionedMixin, models.Model):
    class Meta:
        abstract = True

    version = VersionField()

try:
    from south.modelsinspector import add_introspection_rules
    add_introspection_rules([], ["^ool\.VersionField"])
except ImportError as e:
    pass
