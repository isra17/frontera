from __future__ import absolute_import
from tests.mock import Mock
from frontera import Settings
from frontera.contrib.backends import CommonBackend


class DummyManager:
    def __init__(self, settings):
        self.settings = settings

class Backend(CommonBackend):
    def __init__(self, settings):
        self.manager = DummyManager(settings)

    @property
    def metadata(self):
        return Mock()
    @property
    def queue(self):
        return Mock()
    @property
    def states(self):
        return Mock()

def test_overused():
    settings = Settings(attributes={
        'OVERUSED_BATCH_DELAY': 2
    })

    backend = Backend(settings)
    backend.frontier_start()
    backend.set_overused(0, ['a'])
    assert backend.get_overused_for_batch([0, 1]) == {0: {'a'}, 1: set()}

    backend.set_overused(0, ['a', 'b'])
    assert backend.get_overused_for_batch([0, 1]) == {0: {'a', 'b'}, 1: set()}
    backend.set_overused(1, ['a'])
    assert backend.get_overused_for_batch([0, 1]) == {0: {'b'}, 1: {'a'}}
    assert backend.get_overused_for_batch([0, 1]) == {0: set(), 1: {'a'}}
    assert backend.get_overused_for_batch([0, 1]) == {0: set(), 1: set()}
