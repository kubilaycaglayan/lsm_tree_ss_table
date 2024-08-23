require 'spec_helper'
require 'faker'

require_relative '../lib/key_value_store'

RSpec.describe KeyValueStore do
  before(:each) do
    KeyValueStore.instance.drop_store!
  end

  describe 'Adding more and more data' do
    it 'Adds/reads 1000 key-value pairs' do
      store = KeyValueStore.instance

      data = {}

      (1..1000).each do
        id = Faker::Number.unique.number
        band = Faker::Music::RockBand.name
        store.add(id, band)
        data[id] = band
      end

      data.each do |key, value|
        expect(store.get(key)[:value]).to eq(value)
      end
    end

    it 'Adds/reads 10000 key-value pairs' do
      store = KeyValueStore.instance

      data = {}

      (1..10_000).each do
        id = Faker::Number.unique.number
        band = Faker::Music::RockBand.name
        store.add(id, band)
        data[id] = band
      end

      data.each do |key, value|
        expect(store.get(key)[:value]).to eq(value)
      end
    end

    # Run only when you have time or when optimized
    # it 'Adds/reads 100000 key-value pairs' do
    #   store = KeyValueStore.instance


    #   (1..100000).each do |i|
    #     store.add("color#{i}", "red#{i}")
    #   end

    #   (1..100000).each do |i|
    #     expect(store.get("color#{i}")[:value]).to eq("red#{i}")
    #   end
    # end

    # it 'Adds/reads 1000000 key-value pairs' do
    #   store = KeyValueStore.instance

    #   (1..1000000).each do |i|
    #     store.add("color#{i}", "red#{i}")
    #   end

    #   (1..1000000).each do |i|
    #     expect(store.get("color#{i}")[:value]).to eq("red#{i}")
    #   end
    # end
  end
end
