require 'spec_helper'

require_relative '../lib/key_value_store'

RSpec.describe KeyValueStore do
  before(:each) do
    KeyValueStore.instance.drop_store!
  end

  describe 'SSTable transfer' do
    it "Adds multiple(#{Constants::MEMTABLE_SIZE_LIMIT}) key-value pairs successfully and can read them (update limit))" do
      store = KeyValueStore.instance

      (1..Constants::MEMTABLE_SIZE_LIMIT).each do |i|
        store.add("color#{i}", "red#{i}")
      end

      (1..Constants::MEMTABLE_SIZE_LIMIT).each do |i|
        expect(store.get("color#{i}")[:value]).to eq("red#{i}")
      end
    end

    it "Can add more than #{Constants::MEMTABLE_SIZE_LIMIT}(31) key-value pair limit" do
      store = KeyValueStore.instance

      (1..Constants::MEMTABLE_SIZE_LIMIT + 1).each do |i|
        store.add("color#{i}", "red#{i}")
      end

      (1..Constants::MEMTABLE_SIZE_LIMIT + 1).each do |i|
        expect(store.get("color#{i}")[:value]).to eq("red#{i}")
      end
    end
  end
end
