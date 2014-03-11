from flask.ext.login import UserMixin, AnonymousUserMixin

class User(UserMixin):
    def __init__(self, email):
        self.email = email

    def is_authenticated(self):
        return self.email != None

    def is_authorized(self):
        return self.email.endswith('mozilla.com') or self.email.endswith('mozilla.org')

    def is_active(self):
        return self.email != None

    def is_anonymous(self):
        return self.email == None

    def get_id(self):
        return self.email

class AnonymousUser(AnonymousUserMixin):
    def is_authorized(self):
        return False