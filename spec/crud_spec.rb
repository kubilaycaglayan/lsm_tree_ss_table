require 'spec_helper'
require 'faker'

require_relative '../lib/key_value_store'

RSpec.describe 'CRUD' do
  before(:each) do
    KeyValueStore.instance.drop_store!
  end

  describe 'CRUD operations' do
    it 'Adds the key-value pair' do
      store = KeyValueStore.instance
      id = Faker::Number.unique.number
      band = Faker::Music::RockBand.name
      store.add(id, band)
      result = store.get(id)

      expect(result[:value]).to eq(band)
    end

    it 'Adds the key-value pair - doesn\'t return a value that wasn\'t added' do
      store = KeyValueStore.instance
      name = Faker::Name.unique.name
      color = Faker::Color.color_name
      store.add(name, color)
      result = store.get(name)

      expect(result[:value]).not_to eq('Mandarine')
    end

    it 'Updates the key-value pair' do
      store = KeyValueStore.instance
      name = Faker::Name.unique.name
      color = Faker::Color.color_name
      store.add(name, color)
      store.update(name, 'Mandarine')
      result = store.get(name)

      expect(result[:value]).to eq('Mandarine')
    end

    it 'Deletes the key-value pair' do
      store = KeyValueStore.instance
      name = Faker::Name.unique.name
      color = Faker::Color.color_name
      store.add(name, color)
      store.delete(name)
      result = store.get(name)

      expect(result).to be_nil
    end

    it "Adds multiple(#{Constants::MEMTABLE_SIZE_LIMIT - 1}) key-value pairs successfully and can read them" do
      store = KeyValueStore.instance
      data = {}
      (1..(Constants::MEMTABLE_SIZE_LIMIT - 1)).each do |i|
        key = Faker::Number.unique.number
        value = Faker::Name.name
        store.add(key, value)
        data[key] = value
      end

      data.each do |key, value|
        expect(store.get(key)[:value]).to eq(value)
      end
    end
  end
end
