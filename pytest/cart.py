class Cart:
    def __init__(self):
        self.items = []

    def add_item(self, item, price, quantity=1):
        self.items.append({"item": item, "price": price, "quantity": quantity})

    def remove_item(self, item):
        self.items = [i for i in self.items if i["item"] != item]

    def get_total(self):
        return sum(item["price"] * item["quantity"] for item in self.items)

    def clear_cart(self):
        self.items = []