"""Helper functions

Consists of functions to typically be used within templates, but also
available to Controllers. This module is available to templates as 'h'.
"""

from webhelpers.pylonslib import Flash as _Flash
flash = _Flash('flash')
error = _Flash('error')
