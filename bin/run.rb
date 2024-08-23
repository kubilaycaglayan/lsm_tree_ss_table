require 'faker'

require_relative '../lib/key_value_store'

key_value_store = KeyValueStore.instance

# Create a hash with 1000 faker name and age data
data = {}
(1..1000).each do
  data[Faker::Name.unique.name] = Faker::Number.number(digits: 2)
end


(0..(data.size - 1)).to_a.reverse.each do |i|
  key_value_store.add("#{data.to_a[i][0]}", "#{data.to_a[i][1]}")
end


# Retrieve data
puts "Value for 'key1' (#{data.keys.first}): #{key_value_store.get(data.keys.first)}"
puts "Value for 'key2' (#{data.keys.last}): #{key_value_store.get(data.keys.last)}"

# # Delete data
key_value_store.delete(data.keys.first)

# # Retrieve deleted data
puts "Value for 'key1' after delete (#{data.keys.first}) is nil?: #{key_value_store.get(data.keys.first).nil?}"

# update data
n = Faker::Number.number(digits: 2)
key_value_store.update(data.keys.last, n)
puts n
puts "Value for 'key2' after update (#{data.keys.last}): #{key_value_store.get(data.keys.last)}"
