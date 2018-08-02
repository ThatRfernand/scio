package com.spotify.scio.util

import java.util.concurrent.TimeUnit

import com.google.common.collect.Lists
import com.twitter.algebird.{Monoid, Semigroup}
import org.apache.beam.sdk.testing.CombineFnTester
import org.apache.beam.sdk.transforms.Combine.CombineFn
import org.openjdk.jmh.annotations._

import scala.collection.JavaConverters._

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class FunctionsBenchmark {

  type T = Set[Int]

  val input = Lists.newArrayList((1 to 100).map(Set(_)).asJava)
  val output = (1 to 100).toSet

  val aggregateFn = Functions.aggregateFn[T, T](Set.empty[Int])(_ ++ _, _ ++ _)
  val combineFn = Functions.combineFn[T, T](identity, _ ++ _, _ ++ _)
  val reduceFn = Functions.reduceFn((x: T, y: T) => x ++ y)
  val sgFn = Functions.reduceFn(Semigroup.setSemigroup[Int])
  val monFn = Functions.reduceFn(Monoid.setMonoid[Int])

  def test(fn: CombineFn[T, _, T], input: java.util.List[T], output: T): T = {
    CombineFnTester.testCombineFn(fn, input, output)
    output
  }

  @Benchmark def benchAggregate: T = test(aggregateFn, input, output)
  @Benchmark def benchCombine: T = test(combineFn, input, output)
  @Benchmark def benchReduce: T = test(reduceFn, input, output)
  @Benchmark def benchSemigroup: T = test(sgFn, input, output)
  @Benchmark def benchMonoid: T = test(monFn, input, output)

}
