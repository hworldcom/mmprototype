# Mathematical Foundations - Complete Beginner-Friendly Guide

## PART 1: INTRODUCTION

### Overview

This guide explains ALL the math behind your calibration diagnostics, assuming you might need extra help with statistical concepts. Every acronym is explained, every formula is broken down step-by-step.

**What you'll learn:**
1. Why we use Poisson processes for fill modeling
2. How chi-square tests work (and why)
3. What dispersion means and how to test it
4. How Maximum Likelihood Estimation finds parameters
5. What Bayesian methods add
6. How to detect parameter changes over time

---

## GLOSSARY OF TERMS

Before we dive in, here are ALL the terms you'll encounter:

**Statistical Terms:**
- **Mean (μ)**: Average value. Formula: (x₁ + x₂ + ... + xₙ)/n
- **Variance (σ²)**: How spread out data is. Formula: average of (x - mean)²
- **Standard deviation (σ)**: Square root of variance. Same units as data.
- **Probability (P)**: Chance something happens, between 0 and 1
- **Random variable (X)**: A number that depends on chance
- **Distribution**: Pattern of probabilities (e.g., Normal, Poisson, Exponential)

**Hypothesis Testing:**
- **Null hypothesis (H₀)**: Default assumption we're testing (e.g., "model is correct")
- **Alternative hypothesis (H₁)**: What we suspect instead (e.g., "model is wrong")
- **p-value**: If H₀ is true, probability of seeing data this extreme
- **Significance level (α)**: Cutoff for p-value, usually 0.05 (5%)
- **Type I error**: False alarm - rejecting H₀ when it's true
- **Type II error**: Miss - not rejecting H₀ when it's false

**Distributions:**
- **Poisson distribution**: Counts of rare events (like fills)
- **Exponential distribution**: Waiting times between events
- **Normal distribution**: Bell curve (heights, measurement errors)
- **Chi-square distribution**: Sum of squared normal variables

**Greek Letters:**
- λ (lambda): Rate (events per second)
- δ (delta): Distance or difference
- μ (mu): Mean
- σ (sigma): Standard deviation
- χ² (chi-square): Test statistic or distribution
- ν (nu): Degrees of freedom
- θ (theta): Generic parameter

**Acronyms:**
- **PDF**: Probability Density Function - formula for probability
- **CDF**: Cumulative Distribution Function - P(X ≤ x)
- **MLE**: Maximum Likelihood Estimation - finding best parameters
- **MCMC**: Markov Chain Monte Carlo - sampling from complex distributions
- **CUSUM**: Cumulative Sum - detecting changes
- **CI**: Confidence Interval - range of likely values
- **SE**: Standard Error - uncertainty in an estimate

---

## PART 2: POISSON PROCESSES FOR FILLS

### What is a Poisson Process?

**Simple explanation**: Imagine fills arriving randomly at your orders. Sometimes you get filled quickly, sometimes you wait. A Poisson process is the mathematical model for this randomness.

**Key properties:**
1. Events happen one at a time (no simultaneous fills)
2. Rate is constant over time (or depends only on conditions like distance)
3. Past doesn't affect future (memoryless)
4. Events are independent

**Real example**: Radioactive decay, phone calls to a call center, customers arriving at a store, **fills arriving at your limit orders**.

### The Poisson Distribution

If fills arrive at rate λ per second, the number of fills N in T seconds follows:

**Formula:**
```
P(N = k) = (λT)^k × e^(-λT) / k!
```

**What each part means:**
- k = number of fills (0, 1, 2, 3, ...)
- λT = expected number of fills
- e = 2.718... (mathematical constant)
- k! = k factorial = k × (k-1) × (k-2) × ... × 1

**Properties:**
- **Mean**: E[N] = λT (average fills)
- **Variance**: Var[N] = λT (spread of fills)
- **Special!**: Mean = Variance (this is what we test!)

**Example**: If λ = 0.5 fills/sec for T = 10 sec:
- Expected: λT = 5 fills
- P(exactly 3 fills) = 5³ × e^(-5) / 3! = 125 × 0.0067 / 6 ≈ 14%
- P(exactly 5 fills) = 5^5 × e^(-5) / 5! ≈ 17.5%
- P(0 fills) = e^(-5) ≈ 0.67%

### The Avellaneda-Stoikov Model

**Problem**: Fill rate isn't constant - it depends on how far your order is from mid price!

**Solution**: Model rate as exponentially decreasing with distance:

```
λ(δ) = A × e^(-k×δ)
```

**Parameters:**
- **A**: Fill intensity at mid price (δ=0)
  - Units: fills per second
  - Larger A → more liquid market
  - Typical: 0.1 to 5 for crypto

- **k**: Decay rate
  - Units: 1/tick
  - Larger k → fills very sensitive to distance
  - Typical: 0.1 to 1.0

- **δ**: Distance from mid (in ticks)
  - δ=0: order at mid
  - δ=5: order 5 ticks away

**Why exponential?**
1. Matches empirical data well
2. Has theoretical justification (queue depth, adverse selection)
3. Simple to work with mathematically

**Visual example:**
```
If A=2.0, k=0.3:
δ=0:  λ = 2.0 × e^0      = 2.0 fills/sec
δ=1:  λ = 2.0 × e^(-0.3) = 1.48 fills/sec  (-26%)
δ=3:  λ = 2.0 × e^(-0.9) = 0.81 fills/sec  (-59%)
δ=5:  λ = 2.0 × e^(-1.5) = 0.45 fills/sec  (-78%)
δ=10: λ = 2.0 × e^(-3.0) = 0.10 fills/sec  (-95%)
```

---

## PART 3: CHI-SQUARE TEST IN DETAIL

### What Question Does It Answer?

**"Does my Poisson model actually fit the data?"**

If the model doesn't fit, your backtest will be unreliable!

### The Basic Idea

We divide data into buckets (different distances δ). For each bucket:
- Count **observed** fills (O)
- Calculate **expected** fills from model (E = λ×T)
- Measure deviation: (O-E)²/E

Sum up all deviations → chi-square statistic

If deviations are small → model fits
If deviations are large → model doesn't fit

### Step-by-Step Walkthrough

**Example data:**
| Bucket | δ (ticks) | Time (sec) | Observed fills | Expected fills |
|--------|-----------|------------|----------------|----------------|
| 1      | 1         | 1000       | 62             | 58.3           |
| 2      | 2         | 1000       | 45             | 47.2           |
| 3      | 3         | 1000       | 38             | 38.2           |
| 4      | 5         | 1000       | 22             | 21.8           |
| 5      | 8         | 1000       | 9              | 10.1           |

With parameters A=0.065, k=0.12

**Step 1**: Calculate expected for each bucket

For bucket 1: λ = 0.065 × e^(-0.12×1) = 0.065 × 0.887 = 0.0583
Expected = 0.0583 × 1000 = 58.3 fills ✓

**Step 2**: Calculate (O-E)²/E for each

Bucket 1: (62-58.3)²/58.3 = 3.7²/58.3 = 13.69/58.3 = 0.235
Bucket 2: (45-47.2)²/47.2 = (-2.2)²/47.2 = 4.84/47.2 = 0.103
Bucket 3: (38-38.2)²/38.2 = (-0.2)²/38.2 = 0.04/38.2 = 0.001
Bucket 4: (22-21.8)²/21.8 = (0.2)²/21.8 = 0.04/21.8 = 0.002
Bucket 5: (9-10.1)²/10.1 = (-1.1)²/10.1 = 1.21/10.1 = 0.120

**Step 3**: Sum them up

χ² = 0.235 + 0.103 + 0.001 + 0.002 + 0.120 = **0.461**

**Step 4**: Calculate degrees of freedom

ν = (number of buckets) - (parameters estimated) - 1
ν = 5 - 2 - 1 = **2**

(We estimated A and k, that's 2 parameters)

**Step 5**: Find p-value

Look up in chi-square table or use software:
p-value = P(χ²₂ ≥ 0.461) = **0.794** (79.4%)

**Step 6**: Interpret

p = 0.794 > 0.05 → **PASS**

This means: "If the model is correct, there's a 79.4% chance we'd see this much deviation or more. Not surprising at all!"

**Conclusion**: Model fits the data well ✓

### What Different Results Mean

**p > 0.10**: Excellent fit
- Deviations are totally normal
- High confidence in model

**0.05 < p < 0.10**: Good fit
- Some deviations, but acceptable
- Model is probably fine

**0.01 < p < 0.05**: Marginal fit
- Evidence against model
- Use with caution, consider alternatives

**p < 0.01**: Poor fit
- Strong evidence model is wrong
- Don't use for backtesting!
- Investigate: time-varying? wrong model? data issues?

---

## PART 4: DISPERSION TEST EXPLAINED

### The Core Concept

**Poisson's special property:**
```
Mean = Variance
```

This is unusual! Most distributions have variance ≠ mean.

**Dispersion index:**
```
D = Variance / Mean
```

- D ≈ 1 → Poisson-like (good!)
- D > 1 → Overdispersed (more variable than Poisson)
- D < 1 → Underdispersed (less variable than Poisson)

### Computing Step by Step

**Data**: Observed fills = {62, 45, 38, 22, 9}

**Step 1**: Calculate mean
```
Mean = (62 + 45 + 38 + 22 + 9) / 5 = 176 / 5 = 35.2
```

**Step 2**: Calculate deviations
```
62 - 35.2 = 26.8
45 - 35.2 = 9.8
38 - 35.2 = 2.8
22 - 35.2 = -13.2
9 - 35.2 = -26.2
```

**Step 3**: Square the deviations
```
26.8² = 718.24
9.8² = 96.04
2.8² = 7.84
(-13.2)² = 174.24
(-26.2)² = 686.44
Sum = 1682.8
```

**Step 4**: Calculate variance
```
Variance = 1682.8 / (5-1) = 1682.8 / 4 = 420.7
```
(We divide by n-1=4, not n=5. This is "Bessel's correction" for unbiased estimation)

**Step 5**: Calculate dispersion index
```
D = Variance / Mean = 420.7 / 35.2 = 11.95
```

**Interpretation**: D = 11.95 ≫ 1

This is **severe overdispersion**! Variance is almost 12× the mean.

**What it means**: Fills are clustering way more than Poisson predicts.

**Why this happens:**
1. Time-varying rates (different times of day have different λ)
2. Market events causing bursts of fills
3. Correlation between fills (one fill triggers more)

**Solution**: Use time-varying model (different parameters for different times)

### Practical Guidelines

| D range | Meaning | Action |
|---------|---------|--------|
| 0.8 - 1.2 | Normal Poisson | ✓ Use constant parameters |
| 1.2 - 2.0 | Mild overdispersion | ⚠ Consider time-varying |
| > 2.0 | Severe overdispersion | ✗ Must use time-varying |
| < 0.8 | Underdispersion | ⚠ Check data quality |

---

## PART 5: MAXIMUM LIKELIHOOD ESTIMATION

### The Big Idea

**Question**: What values of A and k make our observed data most probable?

**Answer**: The MLE (Maximum Likelihood Estimate)

### Likelihood Function Explained

**What is "likelihood"?**

The probability of seeing your data given parameters.

**Example**: If λ=1 fill/sec for 10 sec, probability of seeing exactly 8 fills:
```
P(N=8 | λ=1, T=10) = (10)^8 × e^(-10) / 8! = 0.113 (11.3%)
```

**For multiple buckets**, multiply probabilities:
```
Likelihood = P(bucket 1) × P(bucket 2) × P(bucket 3) × ...
```

### Why Use Logarithms?

Products of small numbers get TINY:
```
0.1 × 0.05 × 0.02 × 0.08 × 0.01 = 0.0000000008
```

Taking logs:
- Converts products to sums
- Prevents numerical underflow
- Doesn't change location of maximum

**Log-likelihood**:
```
log(L) = log(P₁) + log(P₂) + log(P₃) + ...
```

Much easier to work with!

### Finding the Maximum

We want to find A and k that maximize log-likelihood.

**Method**: Take derivatives, set to zero, solve.

**Problem**: Equations are nonlinear (no simple solution)

**Solution**: Numerical optimization (computer does it)

**Common algorithms:**
1. Nelder-Mead (what we use)
2. BFGS
3. Newton-Raphson

**How they work**: 
- Start with initial guess
- Evaluate likelihood at nearby points
- Move towards better values
- Repeat until convergence

### Interpretation

**Fitted values**: Â = 1.234, k̂ = 0.289

These are the parameter values that make your observed data **most probable**.

**Confidence intervals** tell you uncertainty:
```
A = 1.234 ± 0.156  (95% CI: [1.078, 1.390])
k = 0.289 ± 0.042  (95% CI: [0.247, 0.331])
```

---

## PART 6: BAYESIAN METHODS

### When to Use Bayesian Calibration

**Use Bayesian when:**
1. Sample size is small (< 100 fills)
2. You have prior knowledge from other symbols
3. MLE gives unstable results

### What Makes It Different?

**Frequentist (MLE)**:
- Parameters are fixed but unknown
- Data is random
- Find single "best" value

**Bayesian**:
- Parameters are random (have distribution)
- Data is observed (fixed)
- Find entire distribution of values

### Prior Distribution

**What it is**: Your belief about parameters BEFORE seeing data

**Example**:
"Based on ETHUSDT, I believe A is probably around 1.5 with uncertainty ±0.5"

**Mathematically**:
```
A ~ LogNormal(log(1.5), 0.5)
```

This says: "A is probably between 1.0 and 2.5, centered at 1.5"

### Posterior Distribution

**Bayes' Rule**:
```
Posterior = (Likelihood × Prior) / Normalization
```

**In words**: 
"Update my beliefs using data"

**Result**: Full distribution of possible A and k values

**Output**:
- A has mean 1.35, 95% credible interval [1.12, 1.58]
- k has mean 0.31, 95% credible interval [0.24, 0.38]

### Why It Helps With Small Samples

**Problem**: With 30 fills, MLE might give A=0.2 or A=5.0 (unstable!)

**Solution**: Prior pulls estimate toward reasonable values

**Think of it as**:
- Data says: "Based on these 30 fills, A could be anything from 0.1 to 10"
- Prior says: "But other symbols suggest A is around 1.5"
- Posterior says: "Compromise: A is probably around 1.3"

This is called **regularization** - prevents overfitting to sparse data.

---

## PART 7: CUSUM CHANGEPOINT DETECTION

### What Problem Does It Solve?

**Problem**: Parameters A and k might change over time

**Question**: When do these changes occur?

**Answer**: CUSUM detects regime shifts

### How CUSUM Works

**Basic idea**: Accumulate deviations from baseline

**Formula**:
```
S(t) = max(0, S(t-1) + (x(t) - μ₀ - K))
```

**Components**:
- x(t) = current parameter value
- μ₀ = baseline (historical average)
- K = allowance for noise
- S(t) = cumulative sum

**Trigger**: When S(t) > threshold h

### Example

**Baseline**: A usually around 1.5

**Observations**: {1.4, 1.6, 1.5, 1.5, 2.3, 2.4, 2.5, 2.2}

**CUSUM with K=0.3, h=2.0**:

| t | A(t) | x-μ₀ | x-μ₀-K | S(t) | Alert? |
|---|------|------|--------|------|--------|
| 1 | 1.4  | -0.1 | -0.4   | 0    | No     |
| 2 | 1.6  | +0.1 | -0.2   | 0    | No     |
| 3 | 1.5  | 0    | -0.3   | 0    | No     |
| 4 | 1.5  | 0    | -0.3   | 0    | No     |
| 5 | 2.3  | +0.8 | +0.5   | 0.5  | No     |
| 6 | 2.4  | +0.9 | +0.6   | 1.1  | No     |
| 7 | 2.5  | +1.0 | +0.7   | 1.8  | No     |
| 8 | 2.2  | +0.7 | +0.4   | 2.2  | YES!   |

**Interpretation**: Detected regime change around time 5
(A jumped from ~1.5 to ~2.3)

### What To Do When Change Is Detected

**Options**:
1. Segment data: Calibrate separately before/after changepoint
2. Use time-varying model: Different parameters for different periods
3. Investigate: What happened in market? News? New algorithms?

---

## PART 8: PUTTING IT ALL TOGETHER

### Complete Workflow

**Step 1: Calibrate**
- Fit (A, k) using MLE
- Get point estimates

**Step 2: Test Goodness-of-Fit**
- Run chi-square test
- Check: p-value > 0.05?

**Step 3: Test Dispersion**
- Calculate D = Variance/Mean
- Check: 0.8 < D < 1.2?

**Step 4: Check Stability**
- Calculate rolling parameters
- Run CUSUM
- Check: CV < 0.3? Few changepoints?

**Step 5: Make Decision**

```
IF chi-square PASS AND dispersion OK AND stable:
    → Use constant (A, k)
    
ELIF chi-square PASS AND dispersion OK AND unstable:
    → Use time-varying schedule
    
ELIF chi-square FAIL:
    → Investigate further
    → Consider alternative models
    → Check data quality
```

### Example Decision Tree

```
Start: Fitted A=1.52, k=0.31

↓
Chi-square test: p=0.08
↓
p > 0.05? YES → Continue
↓
Dispersion: D=1.15
↓
0.8 < D < 1.2? YES → Continue
↓
Parameter CV: 0.25
↓
CV < 0.3? YES → Continue
↓
Sample size: 287 fills
↓
> 100? YES
↓
DECISION: ✓ Use constant parameters
         ✓ Ready for backtesting
```

---

## PART 9: COMMON QUESTIONS

**Q: Why 0.05 for significance level?**

A: It's a convention (Ronald Fisher, 1925). Means 5% chance of false rejection. Some fields use 0.01 (stricter) or 0.10 (more lenient).

**Q: What if I have 20 fills total?**

A: Too small for reliable MLE. Options:
1. Use Bayesian with priors from other symbols
2. Collect more data
3. Aggregate across similar conditions

**Q: My chi-square always fails. Why?**

A: Common reasons:
1. Time-varying rates (use shorter windows)
2. Wrong model form (try alternatives)
3. Data quality issues (check for gaps)

**Q: What's better: constant or time-varying?**

A: Depends on market:
- Stable microstructure → Constant is simpler
- Changing conditions → Time-varying is necessary
- Use diagnostics to decide objectively!

**Q: How many buckets do I need?**

A: At least 5, preferably 10+
- Too few: Not enough data
- Too many: Each bucket too sparse

**Q: Can I use this for other exchanges?**

A: Yes! The math is general. But:
- Parameters will differ
- Some exchanges have different fee structures
- Queue dynamics may vary

---

## PART 10: FORMULAS REFERENCE

**Poisson Distribution:**
```
P(N = k) = (λT)^k × e^(-λT) / k!
Mean = Variance = λT
```

**Exponential Distribution:**
```
f(τ) = λ × e^(-λτ)
F(τ) = 1 - e^(-λτ)
Mean = 1/λ
```

**Chi-Square Statistic:**
```
χ² = Σ(Oᵢ - Eᵢ)² / Eᵢ
ν = n - p - 1
p-value = P(χ²ᵥ ≥ χ²obs)
```

**Dispersion Index:**
```
D = s² / mean
where s² = Σ(xᵢ - x̄)² / (n-1)
```

**KS Statistic:**
```
D = max|F̂(x) - F(x)|
```

**Log-Likelihood (Poisson):**
```
ℓ(A,k) = ΣNᵢlog(λᵢTᵢ) - ΣλᵢTᵢ
where λᵢ = A×exp(-k×δᵢ)
```

**CUSUM:**
```
S⁺(t) = max(0, S⁺(t-1) + xₜ - μ₀ - K)
S⁻(t) = min(0, S⁻(t-1) + xₜ - μ₀ + K)
Alarm if |S⁺| > h or |S⁻| > h
```

---

## FINAL THOUGHTS

**Key Takeaways:**

1. **Chi-square** tells you if model fits overall
2. **Dispersion** tells you if Poisson assumption holds
3. **MLE** finds best parameters statistically
4. **Bayesian** adds robustness for small samples
5. **CUSUM** detects when parameters change

**Don't skip diagnostics!**

Without validation, you're:
- ✗ Backtesting with wrong assumptions
- ✗ Sizing positions incorrectly
- ✗ Misunderstanding your risk
- ✗ Setting yourself up for surprises in live trading

**With diagnostics, you know:**
- ✓ When model fits (and when it doesn't)
- ✓ Whether parameters are stable
- ✓ How uncertain your estimates are
- ✓ When to use time-varying vs constant

**This lets you make informed decisions** about your market making strategy!

---

**Questions?** Review the relevant section above, or check the code implementation in `poisson_diagnostics.py`!

