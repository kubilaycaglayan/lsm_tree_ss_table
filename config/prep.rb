require_relative 'paths'

def create_directories_if_not_exist
  create_data_dir
  create_wal_dir
end

def create_data_dir
  return if Dir.exist?(Paths::DATA_DIR_PATH)

  Dir.mkdir(Paths::DATA_DIR_PATH)
  puts 'Data directory created'
end

def create_wal_dir
  return if Dir.exist?(Paths::WAL_DIR_PATH)

  Dir.mkdir(Paths::WAL_DIR_PATH)
  puts 'WAL directory created'
end
