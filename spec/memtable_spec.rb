require 'spec_helper'

require_relative '../lib/key_value_store'

RSpec.describe KeyValueStore do
  after(:each) do
    # KeyValueStore.instance.drop_store
  end

  describe 'CRUD operations' do
    it 'Adds the key-value pair' do
      store = KeyValueStore.instance
      store.add('color', 'red')
      result = store.get('color')

      expect(result[:value]).to eq('red')
    end

    it 'Adds the key-value pair - doesn\'t return a value that wasn\'t added' do
      store = KeyValueStore.instance
      store.add('color', 'red')
      result = store.get('color')

      expect(result[:value]).not_to eq('white')
    end

    it 'Updates the key-value pair' do
      store = KeyValueStore.instance
      store.add('color', 'red')
      store.update('color', 'white')
      result = store.get('color')

      expect(result[:value]).to eq('white')
    end

    it 'Deletes the key-value pair' do
      store = KeyValueStore.instance
      store.add('color', 'red')
      store.delete('color')

      expect(store.get('color')).to eq(nil)
    end

    it "Adds multiple(#{Constants::MEMTABLE_SIZE_LIMIT - 1}) key-value pairs successfully and can read them" do
      store = KeyValueStore.instance

      (1..(Constants::MEMTABLE_SIZE_LIMIT - 1)).each do |i|
        store.add("color#{i}", "red#{i}")
      end

      (1..(Constants::MEMTABLE_SIZE_LIMIT - 1)).each do |i|
        expect(store.get("color#{i}")[:value]).to eq("red#{i}")
      end
    end
  end

  describe 'SSTable transfer' do
    it "Adds multiple(#{Constants::MEMTABLE_SIZE_LIMIT}) key-value pairs successfully and can read them" do
      store = KeyValueStore.instance

      (1..Constants::MEMTABLE_SIZE_LIMIT).each do |i|
        store.add("color#{i}", "red#{i}")
      end

      (1..Constants::MEMTABLE_SIZE_LIMIT).each do |i|
        expect(store.get("color#{i}")[:value]).to eq("red#{i}")
      end
    end

    it "Can add more than #{Constants::MEMTABLE_SIZE_LIMIT}(31) key-value pairs" do
      store = KeyValueStore.instance

      (1..Constants::MEMTABLE_SIZE_LIMIT + 1).each do |i|
        store.add("color#{i}", "red#{i}")
      end

      (1..Constants::MEMTABLE_SIZE_LIMIT + 1).each do |i|
        expect(store.get("color#{i}")[:value]).to eq("red#{i}")
      end
    end
  end

  describe 'Performance' do
    it 'Adds 1000 key-value pairs' do
      store = KeyValueStore.instance

      (1..1000).each do |i|
        store.add("color#{i}", "red#{i}")
      end

      (1..1000).each do |i|
        expect(store.get("color#{i}")[:value]).to eq("red#{i}")
      end
    end

    # it 'Adds 10000 key-value pairs' do
    #   store = KeyValueStore.instance

    #   (1..10000).each do |i|
    #     store.add("color#{i}", "red#{i}")
    #   end

    #   (1..10000).each do |i|
    #     expect(store.get("color#{i}")[:value]).to eq("red#{i}")
    #   end
    # end
  end

  describe 'Sorted strings' do
    # it 'sorts the keys correctly when added unsorted' do
    #   store = KeyValueStore.instance
    #   store.add('color', 'red')
    #   store.add('animal', 'dog')
    #   store.add('fruit', 'apple')

    #   expect(store.get('animal')[:value]).to eq('dog')
    # end
  end
end
