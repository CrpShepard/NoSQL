import redis

redis_cli = redis.Redis(
	host="localhost", 
	port=6379, 
	db=1)

redis_cli.hmset(
    "product:1",
    {"name": "Bread",
    "price": 50}
)
redis_cli.hmset(
    "product:2",
    {"name": "Milk",
    "price": 120}
)
redis_cli.hmset(
    "product:3",
    {"name": "Egg",
    "price": 150}
)
redis_cli.hmset(
    "product:4",
    {"name": "Tea",
    "price": 130}
)
redis_cli.hmset(
    "product:5",
    {"name": "Coffee",
    "price": 200}
)
redis_cli.hmset(
    "product:6",
    {"name": "Butter",
    "price": 250}
)

redis_cli.rpush(
    "customer:1:name",
    "Alice"
)
redis_cli.rpush(
    "customer:1:purchases",
    *["Bread", "Milk", "Egg"]
)
redis_cli.rpush(
    "customer:2:name",
    "Bob"
)
redis_cli.rpush(
    "customer:2:purchases",
    *["Tea", "Egg"]
)
redis_cli.rpush(
    "customer:3:name",
    "Connor"
)
redis_cli.rpush(
    "customer:3:purchases",
    *["Coffee", "Milk", "Egg"]
)

bought_product = {}

product_keys = redis_cli.keys('product:*')
customer_keys = redis_cli.keys('customer:*:name')

print("Product list:")
for key in product_keys:
    name = redis_cli.hget(key, 'name')
    price = redis_cli.hget(key, 'price')
    print(f"Product: {name}, Price: {price}")

    bought_product[name] = 0

print('\n',"Customers names and purchases list:")
top_customers = []
count = 0
for key in customer_keys:
    name = redis_cli.lindex(key, 0)

    newkey = key.decode('utf-8')
    purchases_key = newkey.replace(":name", ":purchases")
    purchases = redis_cli.lrange(purchases_key, 0, -1)

    if len(purchases) > count:
        top_customers.clear()

    if len(purchases) >= count:
        count = len(purchases)
        top_customers.append(name)

    print(f"Name: {name}, Purchases: {purchases}")

    for purchase in purchases:
        if purchase in bought_product:
            bought_product[purchase] = bought_product.get(purchase) + 1

d = dict((k, v) for k, v in bought_product.items() if v > 0)
print('\n',"Bought products", d)

count = 0
top_product = ""
zero_boughts = []
for key in bought_product:
    if bought_product[key] > count:
        count = bought_product[key]
        top_product = key
    if bought_product[key] == 0:
        zero_boughts.append(key)

print('\n',f"Top product: {top_product}")
print('\n',f"Zero bought products: {zero_boughts}")
print('\n',f"Top customers: {top_customers}")

redis_cli.flushall()
redis_cli.close()
