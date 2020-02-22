import logging

import aerospike

config = {
    'hosts': [('127.0.0.1', 3000)]
}


def connected_client():
    client = aerospike.client(config)
    try:
        client.connect()
    except:
        import sys
        print("failed to connect to the cluster with", config['hosts'])
        sys.exit(1)

    return client


def key(customer_id):
    return 'test', 'store', customer_id


def put(client, input_key, record):
    try:
        client.put(input_key, record)
    except Exception as e:
        import sys
        print("error: {0}".format(e), sys.stderr)


def get(client, input_key):
    (key, metadata, record) = client.get(input_key)
    return key, metadata, record


def add_customer(client, customer_id, phone_number, lifetime_value):
    put(
        client,
        key(customer_id),
        {'phone': phone_number, 'ltv': lifetime_value}
    )


def get_ltv_by_id(client, customer_id):
    try:
        _, _, item = get(client, key(customer_id))
    except aerospike.exception.RecordNotFound:
        logging.error('Requested non-existent customer ' + str(customer_id))
    else:
        return item.get('ltv')


def get_ltv_by_phone(client, phone_number):
    query = client.query('test', 'store')
    query.select('ltv')
    query.where(aerospike.predicates.equals('phone', phone_number))
    records = query.results({'total_timeout': 3000})
    if not records:
        logging.error('Requested phone number is not found ' + str(phone_number))
    return records[0][2]['ltv']


def main():
    aerospike_client = connected_client()

    for i in range(0, 1000):
        add_customer(aerospike_client, i, i, i + 1)

    aerospike_client.index_integer_create("test", "store", "phone", "phone_idx")

    for i in range(0, 1000):
        assert (i + 1 == get_ltv_by_id(aerospike_client, i)), "No LTV by ID " + str(i)
        assert (i + 1 == get_ltv_by_phone(aerospike_client, i)), "No LTV by phone " + str(i)

    aerospike_client.close()


if __name__ == "__main__":
    main()
