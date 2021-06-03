require 'uri'
require 'socket'

class ILP
  def self.connect(uri)
    uri = URI uri
    uri.scheme == 'tcp' or raise "unhandled protocol"
    uri.path.to_s.sub(%r{^/}, "").empty? or raise "path is not used"

    opts = {}
    query = Hash[URI.decode_www_form uri.query || ""]
    query.delete("time_prec")&.then { opts[:time_prec] = _1 }
    query.empty? or raise "unhandled query params: #{query.keys * ", "}"

    io = TCPSocket.new uri.host, uri.port
    new io, **opts
  end

  def initialize(io, time_prec:)
    @io = io
    @point_class = Class.new(Point).tap do |cls|
      cls.time_conv = TimeConv.new time_prec
    end
  end

  attr_reader :io
  def time_conv = @point_class.time_conv

  def write_points(points)
    now = nil
    lines = ""
    points.each do |attrs|
      pt = @point_class.new **attrs
      pt.timestamp ||= now ||= time_conv.now
      lines << "#{pt}\n"
    end
    begin
      @io.write lines
    rescue SystemCallError
      raise WriteError
    end
  end

  class Error < StandardError; end
  class WriteError < Error; end
  class EmptyValuesError < Error; end

  class Point
    def initialize(series:, tags: {}, values:, timestamp: nil)
      @series, @tags, @values, @timestamp = series, tags, values, timestamp
    end

    attr_reader :series, :tags, :values
    attr_accessor :timestamp
  
    def to_s
      s = "#{Esc.esc @series, Esc::MEASUREMENT}"
      @tags.each do |k,v|
        v = v.to_s
        next if v.empty?
        s << ",#{Esc.esc k, Esc::TAG_KEY}=#{Esc.esc v, Esc::TAG_VALUE}"
      end
      s << " " << @values.
        tap { raise EmptyValuesError if _1.empty? }.
        map { |k,v| "#{Esc.esc k, Esc::FIELD_KEY}=#{Esc.field_value v}" }.
        join(",")
      if @timestamp
        s << " " << self.class.time_conv.coerce(@timestamp).to_i.to_s
      end
      s
    end
    
    class << self
      attr_writer :time_conv
      def time_conv; @time_conv or raise "time_conv not set" end
    end

    module Esc
      MEASUREMENT = /[ ,]/
      # https://questdb.io/docs/guides/influxdb-line-protocol/#naming-restrictions
      TAG_KEY = FIELD_KEY = %r{[ \.\?,:\\/\0\)\(_\+\*~%=]}
      TAG_VALUE = /[ ,=]/
      FIELD_VALUE = /[\\"]/

      # https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_reference/#data-types
      def self.field_value(v)
        case v
        when Integer then "#{v}i"
        when String then %{"#{esc v, FIELD_VALUE}"}
        else v.to_s
        end
      end

      def self.esc(s, re)
        s.to_s.gsub(re, "\\\\\\&")
      end
    end
  end

  class TimeConv
    def initialize(precision)
      @factor = FACTORS.fetch(precision) do
        raise ArgumentError, "invalid time precision"
      end
      @clock = Clock.new *CLOCK_NAMES.fetch(precision)
    end

    def now = @clock.get
    def conv(time)= Timestamp.new(time.to_r * @factor)
    
    def coerce(val)
      case val
      when Timestamp then val
      when Time then conv val
      else raise TypeError, "cannot coerce #{val.class}"
      end
    end

    Clock = Struct.new :name, :divisor do
      def get
        time = Process.clock_gettime Process::CLOCK_REALTIME, name
        Timestamp.new time / divisor
      end
    end

    Timestamp = Struct.new :to_i do
      def initialize(val)
        super val.to_i
      end
    end

    # https://github.com/influxdata/influxdb-ruby/blob/master/lib/influxdb/timestamp_conversion.rb
    FACTORS = {
      "n"  => 1e9.to_r,
      "u"  => 1e6.to_r,
      "ms" => 1e3.to_r,
      "s"  => 1.to_r,
      "m"  => 1.to_r / 60,
      "h"  => 1.to_r / 60 / 60,
    }.freeze

    # https://github.com/influxdata/influxdb-ruby/blob/master/lib/influxdb/timestamp_conversion.rb
    CLOCK_NAMES = {
      "n"  => [:nanosecond, 1],
      "u"  => [:microsecond, 1],
      "ms" => [:millisecond, 1],
      "s"  => [:second, 1],
      "m"  => [:second, 60.to_r],
      "h"  => [:second, (60 * 60).to_r],
    }.freeze

    FACTORS.keys == CLOCK_NAMES.keys or raise
  end
end
