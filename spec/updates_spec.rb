require 'spec_helper'
require 'faker'

require_relative '../lib/key_value_store'

RSpec.describe 'Update operations' do
  before(:each) do
    KeyValueStore.instance.drop_store!
    Faker::Number.unique.clear
  end

  describe 'Adds multiple data with the same key' do
    it 'Adds 100 value with the same key but only the last added value is read' do
      store = KeyValueStore.instance

      (1..100).each do |i|
        store.add('color', "red#{i}")
      end

      expect(store.get('color')[:value]).to eq('red100')
      expect(store.get('color')[:value]).not_to eq('red1')
    end

    it 'Adds 100 value with the same key but only the last added value is read - use update method' do
      store = KeyValueStore.instance
      store.add('color', "red1")

      (1..100).each do |i|
        store.update('color', "red#{i}")
      end

      expect(store.get('color')[:value]).to eq('red100')
      expect(store.get('color')[:value]).not_to eq('red1')
    end

    it 'Adds 1000 value with the same key but only the last added value is read' do
      store = KeyValueStore.instance

      (1..1000).each do |i|
        store.add('color', "red#{i}")
      end

      expect(store.get('color')[:value]).to eq('red1000')
      expect(store.get('color')[:value]).not_to eq('red100')
      expect(store.get('color')[:value]).not_to eq('red1')
    end

    it 'Adds 1000 value with the same key but only the last added value is read - use update method' do
      store = KeyValueStore.instance
      store.add('color', "red1")

      (1..1000).each do |i|
        store.update('color', "red#{i}")
      end

      expect(store.get('color')[:value]).to eq('red1000')
      expect(store.get('color')[:value]).not_to eq('red100')
      expect(store.get('color')[:value]).not_to eq('red1')
    end
  end

  describe 'Adds 1000, deletes the first' do
    it 'Adds 1000 key-value pairs and deletes the first one' do
      store = KeyValueStore.instance

      data = {}

      (1..1000).each do
        id = Faker::Number.unique.number
        band = Faker::Music::RockBand.name
        store.add(id, band)
        data[id] = band
      end

      first_key = data.keys.first
      store.delete(first_key)

      expect(store.get(first_key)).to be_nil
    end
  end
end
