require 'yaml'
require 'rest-client'
require 'json'

# http://isbndb.com/api/v2/docs
# http://rubydoc.info/gems/rest-client/1.6.7/frames

def load_api_keys
  key_file_name = ENV['HOME']+'/.isbndb.yml'
  unless File.exist?(key_file_name)
    abort("Required key file #{key_file_name} does not exist")
  end
  @keys = YAML::load(File.open(key_file_name))
end

def set_next_key
  if @current_key.nil?
    @current_key = @keys.values[0]
  else
    i = 0
    @keys.values.each do |key|
      i += 1
      break if key == @current_key
    end
    if @keys.values[i].nil?
      abort("no more keys")
    else
      @current_key = @keys.values[i]
    end
  end
  puts "Using API key '#{@current_key}'"
end

def get_books(query, index='combined', page=1)
  begin
    RestClient.get(BASE_URL, {:params => {:q => query, :i => index, :p => page, :opt => 'keystats'}}) { |response, request, result, &block|
      case response.code
        when 200
          JSON.parse(response)['data'].each do |book|
            @json << book
          end
        else
          response.return!(request, result, &block)
      end
    }
  rescue => e
    puts e.response
  end
end

def open_json
  if File.exist?(JSON_FILE_NAME)
    @json = JSON.parse(IO.read(JSON_FILE_NAME))
  else
    @json = Array.new
  end
end

def write_json
  File.open(JSON_FILE_NAME, "w") do |f|
    f.write(@json.to_json)
  end
end

load_api_keys
set_next_key

BASE_URL="http://isbndb.com/api/v2/json/#{@current_key}/books"
JSON_FILE_NAME='books2.json'

open_json
get_books 'Manning'
write_json

