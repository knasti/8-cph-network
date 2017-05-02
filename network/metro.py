from database import CursorFromConnectionFromPool


class Metro:
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "<Metro {}>".format(self.value)