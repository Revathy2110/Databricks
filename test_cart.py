import pytest
from cart import Cart

@pytest.fixture
def cart():
    return Cart()

def test_add_item(cart):
    cart.add_item("Laptop", 1000, 2)
    cart.add_item("Phone", 500)
    assert len(cart.items) == 2
    assert cart.items[0]["item"] == "Laptop"
    assert cart.items[0]["price"] == 1000
    assert cart.items[0]["quantity"] == 2

def test_remove_item(cart):
    cart.add_item("Laptop", 1000)
    cart.add_item("Phone", 500)
    cart.remove_item("Laptop")
    assert len(cart.items) == 1
    assert cart.items[0]["item"] == "Phone"

def test_get_total(cart):
    cart.add_item("Laptop", 1000, 2)
    cart.add_item("Phone", 500)
    total = cart.get_total()
    assert total == 2500

def test_clear_cart(cart):
    cart.add_item("Laptop", 1000)
    cart.add_item("Phone", 500)
    cart.clear_cart()
    assert len(cart.items) == 0



