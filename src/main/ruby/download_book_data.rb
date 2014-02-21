require 'rest-client'
require 'json'

@json = Array.new
for i in 1..20
  http_response = RestClient.get "http://isbndb.com/api/v2/json/XXXXX/books?q=Manning&i=publisher_name&p=#{i}&opt=keystats"
  raw_data = JSON.parse(http_response)
  raw_data['data'].each do |book|
    @json << book
  end
end
File.open("books.json", "w") do |f|
  f.write(@json.to_json)
end
