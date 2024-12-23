class E1(Exception):
    def __init__ (self):
        super().__init__('Title length should range from 1 to 100 characters')

class E2(Exception):
    def __init__ (self):
        super().__init__('Director length should range from 1 to 50 characters')

class E3(Exception):
    def __init__ (self, title, director):
        super().__init__(f'DVD ({title}, {director}) already exists')

class E4(Exception):
    def __init__ (self):
        super().__init__('Username length should range from 1 to 50')

class E5(Exception):
    def __init__ (self, d_id):
        super().__init__(f'DVD {d_id} does not exist')

class E6(Exception):
    def __init__ (self):
        super().__init__('Cannot delete a DVD that is currently borrowed')

class E7(Exception):
    def __init__ (self, u_id):
        super().__init__(f'User {u_id} does not exist')

class E8(Exception):
    def __init__ (self):
        super().__init__('Cannot delete a user with borrowed DVDs')

class E9(Exception):
    def __init__ (self):
        super().__init__('Cannot check out a DVD that is out of stock')

class E10(Exception):
    def __init__ (self, u_id):
        super().__init__(f'User {u_id} exceeded the maximum borrowing limit')

class E11(Exception):
    def __init__ (self):
        super().__init__('Rating should range from 1 to 5 integer')

class E12(Exception):
    def __init__ (self):
        super().__init__('Cannot return and rate a DVD that is not currently borrowed for this user')

class E13(Exception):
    def __init__ (self, name, age):
        super().__init__(f'({name}, {age}) already exists')

class E14(Exception):
    def __init__ (self):
        super().__init__('Age should be a positive integer')

class E15(Exception):
    def __init__ (self):
        super().__init__('User cannot borrow same DVD simultaneously')

class E16(Exception):
    def __init__ (self):
        super().__init__('Cannot find any matching results')












