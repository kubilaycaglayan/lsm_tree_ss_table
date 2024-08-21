module Paths
  WAL_FILE_PATH = ENV['APP_ENV'] == 'test' ?
    File.expand_path('../spec/wal/wal.log', __dir__) :
    File.expand_path('../wal/wal.log', __dir__)
  WAL_DIR_PATH = ENV['APP_ENV'] == 'test' ?
    File.expand_path('../spec/wal', __dir__) :
    File.expand_path('../wal', __dir__)
  DATA_DIR_PATH = ENV['APP_ENV'] == 'test' ?
    File.expand_path('../spec/data', __dir__) :
    File.expand_path('../data', __dir__)
end
