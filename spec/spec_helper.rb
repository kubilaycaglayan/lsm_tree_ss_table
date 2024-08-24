RSpec.configure do |config|
  system('clear')
  ENV['APP_ENV'] ||= 'test'

  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.shared_context_metadata_behavior = :apply_to_host_groups
  config.profile_examples = 10
  config.order = :random
  config.filter_run_when_matching :focus
  Kernel.srand config.seed
end
