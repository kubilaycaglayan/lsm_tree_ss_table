# exceptions.rb

class KeyNotFoundError < StandardError
  def initialize(msg = "Key not found")
    super
  end
end

class InvalidOperationError < StandardError
  def initialize(msg = "Invalid operation")
    super
  end
end
