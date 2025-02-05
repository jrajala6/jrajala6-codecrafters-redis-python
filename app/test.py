class MyRange:
    def __init__(self, start, stop = None, step = None, /):
        if stop is None:
            stop = start
            start = 0
        if step is None:
            step = 1
        self._require_int(start, 'start')
        self._require_int(stop, 'stop')
        self._require_int(step, 'step')
        self._start = start
        self._stop = stop
        self._step = step

    @staticmethod
    def _require_int(value, name):
        if type(value) is not int:
            raise TypeError(f'{name} must be an integer, but was {value}')

    def start(self):
        return self._start

    def stop(self):
        return self._stop

    def step(self):
        return self._step

    def __iter__(self):
        return MyRangeIterator(self)

class MyRangeIterator:
    def __init__(self, myrange):
        self._myrange = myrange
        self._next = myrange.start()

    def __iter__(self):
        return self

    def __next__(self):
        result = self._next
        self._next += self._myrange.step()
        return result