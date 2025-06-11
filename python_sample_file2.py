class Bankaccount():
    def __init__(self,balance,credit):
        self.balance = balance 
        self.credit = credit 
    def get_adding_amount(self,amount):
        self.balance = self.balance + amount 
    def get_credit_amount(self):
        return self.credit  
    def get_total_amount(self):
        return self.balance + self.credit 

get_result = Bankaccount(1000,2000) 
get_result.get_adding_amount(8000) 
print(get_result.balance)
print(get_result.get_credit_amount())
print(get_result.get_total_amount())