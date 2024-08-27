require 'spec_helper'

require_relative '../lib/key_value_store'
require_relative '../lib/memtable'
require_relative '../config/paths'
require_relative '../config/constants'
require_relative '../utils/file_helper'

wal_path = Paths::WAL_FILE_PATH

RSpec.describe 'Write-ahead log' do
  include FileHelper

  before(:each) do
    KeyValueStore.instance.drop_store!
  end

  describe 'Write-ahead log writes' do
    it 'Correctly writes incoming operations' do
      store = KeyValueStore.instance

      store.add('1', 'red')
      store.add('2', 'blue')
      store.add('3', 'green')
      store.add('4', 'yellow')
      store.add('5', 'orange')

      data = read_from_file(wal_path)

      expect(data['1'][:value]).to eq('red')
      expect(data['2'][:value]).to eq('blue')
      expect(data['3'][:value]).to eq('green')
      expect(data['5'][:value]).to eq('orange')
      expect(data['4'][:value]).to eq('yellow')
    end

    it 'Writes incoming operations within order' do
      store = KeyValueStore.instance

      store.add('1', 'red')
      store.add('2', 'blue')
      store.add('3', 'green')
      store.add('5', 'orange')
      store.add('4', 'yellow')

      data = []
      File.readlines(wal_path).each do |line|
        entry = JSON.parse(line)
        data.push(entry['key'])
      end

      expect(data).to eq(%w[1 2 3 5 4])
    end

    it 'Correctly writes incoming operations (delete, update)' do
      store = KeyValueStore.instance

      store.add('1', 'red')
      store.add('2', 'blue')
      store.add('3', 'green')
      store.add('4', 'yellow')
      store.add('5', 'orange')

      store.delete('1')
      store.update('2', 'purple')

      data = read_from_file(wal_path)

      expect(data['1'][:deleted]).to eq(true)
      expect(data['2'][:value]).to eq('purple')
    end
  end

  describe 'Write-ahead log flushes' do
    it "Flushes after #{Constants::MEMTABLE_SIZE_LIMIT} operations" do
      store = KeyValueStore.instance

      (1..Constants::MEMTABLE_SIZE_LIMIT).each do |i|
        store.add(i.to_s, 'value')
      end

      data = read_from_file(wal_path)

      expect(data.size).to eq(0)
    end
  end

  describe 'Write-ahead log recovers' do
    it 'Recovers data on initialization' do
      File.open(wal_path, 'w') do |file|
        file.puts('{"key":"1HLSA","value":"red","timestamp":"20240824153639"}')
        file.puts('{"key":"2HLSA","value":"blue","timestamp":"20240824153639"}')
        file.puts('{"key":"3HLSA","value":"green","timestamp":"20240824153639"}')
        file.puts('{"key":"4HLSA","value":"yellow","timestamp":"20240824153639"}')
        file.puts('{"key":"5HLSA","value":"orange","timestamp":"20240824153639"}')
      end

      file_data = read_from_file(wal_path)

      expect(file_data['1HLSA'][:value]).to eq('red')

      store = KeyValueStore.instance

      expect(store.get('1HLSA')).to eq(nil)
      MemTable.instance.check_write_ahead_log_and_recover
      expect(store.get('1HLSA')[:value]).to eq('red')
    end
  end
end
