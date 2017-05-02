from database import CursorFromConnectionFromPool


class Bus:
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "<Bus {}>".format(self.value)

    def save_to_db(self):
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute('INSERT INTO users (screen_name, oauth_token, oauth_token_secret) VALUES (%s, %s, %s)',
                           (self.screen_name, self.oauth_token, self.oauth_token_secret))

    @classmethod
    def load_from_db_by_screen_name(cls, screen_name):
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute('SELECT * FROM ')
            bus_data = cursor.fetchone()
            if bus_data:  # None is equilavent to false in boolaen expressions
                return cls(screen_name=bus_data[1], oauth_token=bus_data[2],
                           oauth_token_secret=bus_data[3], id=bus_data[0])