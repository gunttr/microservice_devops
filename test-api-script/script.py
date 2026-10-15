import requests
from time import sleep
import random
import sys
import os


API_ENDPOINT: str = os.getenv("API_ENDPOINT", "http://localhost:8000")

try:
    # how many seconds the script will sleep before next while iteration
    # 60 seconds default
    TIME_TO_SLEEP_SECONDS: int = int(os.getenv("TIME_TO_SLEEP_SECONDS", 60))

    # how many requests per iteration of the while loop the script will send
    # 10 post and get requests default
    REQUESTS_PER_ITERATION: int = int(os.getenv("REQUESTS_PER_ITERATION", 10))
except ValueError:
    print(f"TIME_TO_SLEEP_SECONDS and REQUESTS_PER_ITERATION must be integer")
    sys.exit(1)


products = ["apple", "banana", "cherry pie", "kilka", "cheetos", "coffee", "matcha"]
users = ["vladick", "nastya", "kostyan", "maksos", "igorek", "jeka"]

print("The script has started")

while True:
    print("Waken up from sleep")
    for _ in range(REQUESTS_PER_ITERATION):
        try:
            payload = {
                    "username": random.choice(users),
                    "product_name": random.choice(products),
                    "quantity": random.randint(1, 20)
            }
            post_response = requests.post(
                url=f"{API_ENDPOINT}/orders",
                json=payload,
                timeout=10,
            )

            print("Post request has been sent with this payload")
            print(payload)
            
            post_response.raise_for_status()

            order_id: int = post_response.json().get("order_id")
            get_response = requests.get(
                url=f"{API_ENDPOINT}/orders/{order_id}"
            )
            print("Get request has been sent with such id: ", order_id)

            sleep(5)

        except requests.exceptions.HTTPError as err:
            print(f"HTTP Error occured: {err}")
            continue
        except requests.exceptions.ConnectionError as err:
            print(f"Connection error occured: {err}")
            break
        except requests.exceptions.ReadTimeout as err:
            print(f"The server did not send any data in the allotted amount of time: {err}")
            break
        except requests.exceptions.Timeout as err:
            print(f"The request timed out: {err}")
            break
        except requests.exceptions.RequestException as err:
            print(f"An ambiguous error has occured: {err}")
            break

    print(f"Sleeping for {TIME_TO_SLEEP_SECONDS} seconds")
    sleep(TIME_TO_SLEEP_SECONDS)