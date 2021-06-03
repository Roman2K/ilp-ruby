require 'minitest/autorun'
require_relative 'ilp'

class ILP

class TimeConvTest < MiniTest::Test
  def test_precision
    err = assert_raises ArgumentError do
      TimeConv.new 'xx'
    end
    assert_match /invalid time prec/, err.message

    now = Time.now
    conv = TimeConv.new 's'
    assert_equal now.to_i, conv.now.to_i

    now = Time.now
    conv = TimeConv.new 'n'
    assert_in_delta now.to_f * 1e9, conv.now.to_i, 0.0001 * 1e9

    now = Time.now
    conv = TimeConv.new 'ms'
    assert_equal (now.to_f * 1e3).to_i, conv.conv(now).to_i
    assert_equal (now.to_f * 1e3).to_i, conv.coerce(now).to_i
    assert_equal (now.to_f * 1e3).to_i, conv.coerce(conv.coerce(now)).to_i
  end
end

class PointTest < MiniTest::Test
  def test_attrs
    err = assert_raises ArgumentError do
      Point.new
    end
    assert_match /missing.+:series/, err.message

    Point.new series: "aa", values: {}
  end

  def test_to_s
    pt = Point.new series: "aa", values: {}
    assert_raises EmptyValuesError do
      pt.to_s
    end

    pt = Point.new series: "aa", values: {bb: 1}
    assert_equal %{aa bb=1i}, pt.to_s

    pt = Point.new series: "a a", values: {bb: 1}
    assert_equal %{a\\ a bb=1i}, pt.to_s

    pt = Point.new series: "a a", values: {bb: "xx"}
    assert_equal %{a\\ a bb="xx"}, pt.to_s

    pt = Point.new series: "a", tags: {cc: "dd"}, values: {bb: 1.0}
    assert_equal %{a,cc=dd bb=1.0}, pt.to_s

    pt = Class.new(Point).
      tap { _1.time_conv = TimeConv.new "ms" }.
      new series: "a", values: {bb: 1.0}, timestamp: Time.at(1234)
    assert_equal %{a bb=1.0 1234000}, pt.to_s
  end
end

end
