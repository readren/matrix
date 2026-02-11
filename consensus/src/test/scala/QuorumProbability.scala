package readren.consensus

object QuorumProbability {


	/**
	 * Computes the binomial coefficient C(n, k) = "n choose k".
	 *
	 * @param n total number of trials/items
	 * @param k number of successes/chosen items
	 * @return the binomial coefficient as `Long`
	 * @note returns 0 if `k < 0` or `k > n`
	 * @note uses multiplicative formula to minimize intermediate overflow
	 *       (still limited by Long range)
	 */
	private def binom(n: Int, k: Int): Long = {
		if k < 0 || k > n then 0L
		else {
			var res = 1L
			var i = 0
			while i < k do {
				res *= (n - i)
				res /= (i + 1)
				i += 1
			}
			res
		}
	}

	/**
	 * Computes the probability P(X ≥ m) where X ~ Binomial(n, p).
	 *
	 * This is the less numerically stable version.
	 *
	 * @param n number of independent Bernoulli trials
	 * @param p success probability of each trial (must be ∈ [0,1])
	 * @param m minimum number of successes (threshold)
	 * @return probability that at least `m` successes occur
	 */
	private def probAtLeast(n: Int, p: Double, m: Int): Double = {
		val q = 1.0 - p
		var sum = 0.0
		var term = math.pow(q, n).toDouble // starting point: all failures
		var coeff = 1L

		// We compute cumulatively from k=0 up, but only sum from m
		var k = 0
		while k <= n do {
			if k >= m then sum += term
			k += 1
			if k <= n then {
				coeff = coeff * (n - k + 1) / k
				term = term * (p / q) * coeff / coeff // better numerical stability
				term *= p / q
			}
		}
		sum
	}

	/**
	 * Numerically more stable computation of P(X ≥ m) where X ~ Binomial(n, p).
	 *
	 * Uses recursive probability ratios between consecutive terms instead of
	 * direct power calculations.
	 *
	 * @param n number of independent Bernoulli trials
	 * @param p success probability of each trial (must be ∈ [0,1])
	 * @param m minimum number of successes (threshold)
	 * @return probability of at least `m` successes
	 *
	 * Special cases:
	 *   - returns 0.0 if p ≤ 0
	 *   - returns 1.0 if p ≥ 1
	 */
	private def probAtLeastStable(n: Int, p: Double, m: Int): Double = {
		if p <= 0.0 then 0.0
		else if p >= 1.0 then 1.0
		else {
			val q = 1.0 - p
			var prob = 0.0
			var term = math.pow(q, n)
			var binCoeff = 1L

			for k <- 0 to n do {
				if k >= m then prob += term
				if k < n then {
					binCoeff = binCoeff * (n - k) / (k + 1)
					term = term * p / q * (n - k).toDouble / (k + 1).toDouble
				}
			}
			prob
		}
	}

	/**
	 * Finds the success probability `p` such that the probability of reaching strict majority (at least floor(n/2)+1 successes) equals `pQuorum`.
	 *
	 * In other words: finds p so that P(X ≥ m) = pQuorum where X ~ Binomial(n, p) and m = floor(n/2) + 1
	 *
	 * Uses binary search (bisection method) on the interval [0,1].
	 *
	 * @param n number of nodes/participants/votes (must be ≥ 1)
	 * @param pQuorum desired probability of reaching quorum ∈ [0,1]
	 * @param tolerance desired precision of the result (default: 1e-7)
	 * @param maxIter maximum number of bisection iterations (default: 80)
	 * @return the estimated success probability p ∈ [0,1]
	 *
	 * Special cases:
	 *   - pQuorum ≤ 0.0  → returns 0.0
	 *   - pQuorum ≥ 1.0  → returns 1.0
	 *
	 * @throws IllegalArgumentException if n < 1 or pQuorum ∉ [0,1]
	 *
	 * @note Very accurate for small-to-moderate n (especially n ≤ 20)
	 */
	def findPExito(n: Int, pQuorum: Double, tolerance: Double = 1e-7, maxIter: Int = 80): Double = {
		require(n >= 1, "n must be positive")
		require(pQuorum >= 0.0 && pQuorum <= 1.0, "pQuorum must be in [0,1]")

		val m = n / 2 + 1 // majority threshold: floor(n/2)+1

		if pQuorum <= 0.0 then 0.0
		else if pQuorum >= 1.0 then 1.0
		else {
			var low = 0.0
			var high = 1.0
			var iter = 0

			while (high - low) > tolerance && iter < maxIter do {
				val mid = (low + high) / 2
				val prob = probAtLeastStable(n, mid, m)

				if prob < pQuorum then low = mid
				else high = mid

				iter += 1
			}
			(low + high) / 2
		}
	}

	// ─── Example usage ────────────────────────────────────────────────
	@main def demo(): Unit = {

		val cases = List(
			(3, 0.90) -> "~0.804",
			(3, 0.99) -> "~0.941",
			(5, 0.80) -> "~0.673",
			(5, 0.90) -> "~0.758",
			(7, 0.85) -> "~0.696"
		)

		for ((n, q), expected) <- cases do {
			val p = findPExito(n, q)
			println(f"N=$n, quorum=$q%.2f → p ≈ $p%.4f    (expected $expected)")
		}
	}
}
