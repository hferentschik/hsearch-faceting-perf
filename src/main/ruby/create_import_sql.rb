require 'json'

data = JSON.parse(IO.read("books.json"))
data.each do |book|
  puts "#{book['title']}"
  puts "#{book['publisher_name']}"
  book['author_data'].each do |author|
    puts "#{author['name']}"
  end
  puts ">>>"
  puts ""
end
