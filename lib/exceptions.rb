class KeyNotFoundError < StandardError
  def initialize(key, msg = "Key not found")
    super("#{msg}: #{key} - #{key.class}")
  end
end

