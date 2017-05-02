from database import CursorFromConnectionFromPool


class Pedestrian:
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "<Pedestrian {}>".format(self.value)