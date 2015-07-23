/**
 * 
 */
package istc.bigdawg.utils;

/**
 * @author Adam Dziedzic
 * 
 *         Implement tuples.
 * 
 *         Use it if you need Pairs, Triples and so on (for example for the
 *         return values of specified cardinality and different types).
 * 
 */
public class Tuple {
	public static <T1, T2> Tuple2<T1, T2> tuple2(T1 t1, T2 t2) {
		return new Tuple.Tuple2<T1, T2>(t1, t2);
	}

	public static <T1, T2, T3> Tuple3<T1, T2, T3> tuple3(T1 t1, T2 t2, T3 t3) {
		return new Tuple.Tuple3<T1, T2, T3>(t1, t2, t3);
	}

	public static class Tuple2<T1, T2> {
		protected T1 t1;
		protected T2 t2;

		public Tuple2(T1 f1, T2 f2) {
			this.t1 = f1;
			this.t2 = f2;
		}

		public T1 getT1() {
			return t1;
		}

		public T2 getT2() {
			return t2;
		}
	}

	public static class Tuple3<T1, T2, T3> extends Tuple2<T1, T2> {
		protected T3 t3;

		public Tuple3(T1 f1, T2 f2, T3 f3) {
			super(f1, f2);
			this.t3 = f3;
		}

		public T3 getT3() {
			return t3;
		}
	}
}
