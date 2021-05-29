require 'ilp'

client = ILP.connect "tcp://hetax.srv:9009/testdb?time_prec=n"

time = client.time_conv.now
points = Array.new 1000 do
  { series: "sensors",
    tags: {"room" => %w[a b c].sample},
    values: {temp: 19 + rand},
    timestamp: time }
end

client.write_points points
puts "written #{points.size} points"
