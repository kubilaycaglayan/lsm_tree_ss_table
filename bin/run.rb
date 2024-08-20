require_relative '../config/prep'
require_relative '../lib/key_value_store'

create_directories

key_value_store = KeyValueStore.instance

# Add data to LSM Tree
key_value_store.add('key1', 'value1')
key_value_store.add('key2', 'value2')
key_value_store.add('key3', 'value3')
key_value_store.add('key4', 'value4')

# Retrieve data
puts "Value for 'key1': #{key_value_store.get('key1')}"
puts "Value for 'key2': #{key_value_store.get('key2')}"

# Delete data
key_value_store.delete('key1')

# Retrieve deleted data
puts "Value for 'key1' after delete: #{key_value_store.get('key1')}"
