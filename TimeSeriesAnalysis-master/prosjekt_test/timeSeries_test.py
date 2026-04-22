# Python demo: generate closed-loop PID + first-order plant dataset,
# perform lagged-correlation candidate screening, and run a simple beam-search
# that fits ARX-like single-input models (OLS) and greedily adds edges.
#
# This is a self-contained example to help you start building the full pipeline.
# It does NOT use TimeSeriesAnalysis (TSA) because TSA is a .NET library.
# Instead it demonstrates the concepts (synthetic data, screening, beam search,
# simple identification) in pure Python.
#
# Outputs:
# - Displays a small portion of the dataset as a table
# - Prints candidate edges from screening (top k lags)
# - Runs a greedy beam search that adds edges one-by-one and evaluates
#   one-step-ahead prediction NRMSE on a validation set
# - Plots measured vs predicted outputs for the final model
#
# NOTE: This is a simplified demonstration. For the project you'll swap
# the identification/simulation parts with calls into TSA for richer models,
# closed-loop-aware identification, and rollout-based evaluation.
#
import numpy as np
import polars as pl
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import math

# Replacement for caas_jupyter_tools
def display_dataframe_to_user(filename, df):
    """Simple replacement for the missing caas_jupyter_tools function"""
    print(f"\n--- Data preview from {filename} ---")
    print(df)
    print("-----------------------------------\n")

np.random.seed(1)

# -----------------------------
# 1) Generate synthetic closed-loop data (PID controller + 1st-order plant)
# -----------------------------
T = 2000  # number of timesteps
dt = 1.0  # seconds per step
time = np.arange(T) * dt

# plant parameters (first-order with gain K and time-constant tau, with deadtime d)
K_plant = 2.0
tau = 20.0  # time constant
deadtime = 5  # samples of pure delay for actuator->plant
# true process (discrete Euler approx)
# y_{t+1} = y_t + dt*( -y_t/tau + K * u_delayed / tau ) + noise
# Controller: discrete PID on error e = r - y, output u (with saturations)

# setpoint profile (multi-step changes)
r = np.ones(T) * 1.0
r[200:400] = 2.0
r[400:800] = 0.5
r[800:1200] = 1.5
r[1200:1400] = 1.0
r[1400:] = 1.2

# disturbance (unknown to controller)
disturbance = 0.05 * np.sin(0.01 * np.pi * time) + 0.02 * np.random.randn(T)

# PID parameters (controller in series with actuator delay)
Kp, Ki, Kd = 1.2, 0.01, 2.0
u = np.zeros(T)
y = np.zeros(T)
integral = 0.0
prev_error = 0.0

# actuator saturation
u_min, u_max = -2.0, 2.0

# Simulate closed-loop with deadtime on actuator->plant
u_delayed_buf = [0.0] * (deadtime + 1)  # circular buffer for delay

for t in range(T - 1):
    error = r[t] - y[t]
    integral += error * dt
    derivative = (error - prev_error) / dt
    u_unclipped = Kp * error + Ki * integral + Kd * derivative
    u[t] = max(u_min, min(u_max, u_unclipped))  # saturate
    prev_error = error

    # push into delay buffer and read delayed input
    u_delayed_buf.append(u[t])
    u_delayed = u_delayed_buf.pop(0)

    # plant update (Euler)
    y[t+1] = y[t] + dt * (-y[t] / tau + K_plant * u_delayed / tau) + disturbance[t]

# trim last sample for consistent lengths
u = u[:-1]
y = y[:-1]
r = r[:-1]
disturbance = disturbance[:-1]
time = time[:-1]
T = len(time)

# package into DataFrame
df = pl.DataFrame({
    'time': time,
    'setpoint': r,
    'controller_u': u,
    'plant_y': y,
    'disturbance': disturbance
})

display_dataframe_to_user("synthetic_closed_loop_dataset.csv", df.head(80))

print("Generated synthetic closed-loop dataset: columns =", df.columns)
print(f"Length T = {T}. Plant deadtime (true) = {deadtime} samples.")

# -----------------------------
# 2) Candidate screening via lagged cross-correlation (per output)
# -----------------------------
def lagged_correlation(x, y, max_lag=30):
    # compute Pearson correlation of x(t - lag) with y(t) for lag = 0..max_lag
    corrs = []
    for lag in range(max_lag + 1):
        if lag == 0:
            xs = x
        else:
            xs = x[:-lag]
        ys = y[lag:]
        if len(xs) < 5:
            corrs.append(0.0)
            continue
        corrs.append(np.corrcoef(xs, ys)[0,1])
    return np.array(corrs)

max_lag = 30
candidates_per_output = {}
inputs = ['setpoint', 'controller_u', 'disturbance']

for out in ['plant_y']:
    candidates = []
    y_series = df['plant_y'].to_numpy()
    for inp in inputs:
        x_series = df[inp].to_numpy()
        corrs = lagged_correlation(x_series, y_series, max_lag=max_lag)
        # find top-k lag peaks by absolute corr value
        top_k = 5
        top_idxs = np.argsort(np.abs(corrs))[::-1][:top_k]
        for idx in top_idxs:
            candidates.append((inp, out, int(idx), corrs[idx]))
    # sort candidates by absolute correlation desc
    candidates = sorted(candidates, key=lambda x: abs(x[3]), reverse=True)
    candidates_per_output[out] = candidates

print("\nTop candidate edges (input -> output, lag, corr):")
for out, cand in candidates_per_output.items():
    for c in cand[:8]:
        print(c)
        
# -----------------------------
# 3) Simple beam/greedy search + ARX single-input fit
# We'll do a greedy additive procedure: start with no inputs to the output,
# then at each step try adding each candidate (that isn't already used),
# fit an OLS model for the set of chosen inputs (each with a given lag window),
# and keep the addition that improves validation error the most. Repeat until
# no improvement or max edges reached.
# -----------------------------
# Prepare train/val split for time-series (no shuffling): use first 60% train, next 20% val, last 20% test
n_train = int(0.6 * T)
n_val = int(0.2 * T)
train_idx = slice(0, n_train)
val_idx = slice(n_train, n_train + n_val)
test_idx = slice(n_train + n_val, T)

def build_regressors(df, input_specs, output_name, n_arx=3):
    # input_specs: list of tuples (input_name, lag, n_terms): use terms at lag..lag+n_terms-1
    # also include autoregressive terms of output (n_arx past outputs)
    y = df[output_name].to_numpy()
    N = len(y)
    cols = []
    names = []
    # AR terms (past outputs)
    for k in range(1, n_arx+1):
        cols.append(np.concatenate(([np.nan]*k, y[:-k])))
        names.append(f"y_lag{k}")
    # input terms
    for (inp, lag, n_terms) in input_specs:
        x = df[inp].to_numpy()
        for k in range(n_terms):
            shift = lag + k
            cols.append(np.concatenate(([np.nan]*shift, x[:-shift])))
            names.append(f"{inp}_lag{shift}")
    # stack and drop rows with NaN
    X = np.vstack(cols).T
    valid = ~np.isnan(X).any(axis=1)
    return X[valid], y[valid], names

# fixed ARX term settings
n_arx = 2
n_terms_per_input = 2  # number of tapped delays per chosen input

# candidate list simplified: use top 8 from screening for plant_y
cand_list = [ (c[0], c[2]) for c in candidates_per_output['plant_y'][:8] ]  # (input, lag)
# we'll add inputs one-by-one (keeping their nominated lag), using n_terms_per_input taps each
selected = []  # list of (input, lag)
best_val_err = float('inf')
history = []

max_edges = 3

for step in range(max_edges):
    improvements = []
    for candidate in cand_list:
        if candidate in selected:
            continue
        trial_specs = [(inp, lag, n_terms_per_input) for (inp, lag) in selected + [candidate]]
        X, yvec, names = build_regressors(df, trial_specs, 'plant_y', n_arx=n_arx)
        # split to train/val by indices - need to map valid rows back to indices; use initial valid mask logic
        # rebuild valid mask differently to keep alignment
        def build_masked(df, specs, n_arx=2):
            yfull = df['plant_y'].to_numpy()
            N = len(yfull)
            total_shift = 0
            for (_, lag, _) in specs:
                total_shift = max(total_shift, lag + n_terms_per_input - 1)
            total_shift = max(total_shift, n_arx)
            valid_mask = np.ones(N, dtype=bool)
            valid_mask[:total_shift] = False
            return valid_mask, total_shift
        valid_mask, offset = build_masked(df, trial_specs, n_arx)
        # map train/val/test indices to masked indexing
        idxs = np.where(valid_mask)[0]
        # find index splits in this idxs array corresponding to train/val/test ranges
        # train: first indices that fall into original train_idx slice, etc.
        train_mask = (idxs >= train_idx.start) & (idxs < train_idx.stop)
        val_mask = (idxs >= n_train) & (idxs < n_train + n_val)
        test_mask = (idxs >= n_train + n_val) & (idxs < T)
        if train_mask.sum() < 10 or val_mask.sum() < 5:
            continue  # not enough data to evaluate
        X_train = X[train_mask]; y_train = yvec[train_mask]
        X_val = X[val_mask]; y_val = yvec[val_mask]
        # fit OLS
        model = LinearRegression(fit_intercept=True)
        model.fit(X_train, y_train)
        ypred_val = model.predict(X_val)
        nrmse = math.sqrt(mean_squared_error(y_val, ypred_val)) / (y_val.max() - y_val.min())
        improvements.append((candidate, nrmse, model, names))
    if not improvements:
        break
    # pick best candidate (lowest nrmse)
    improvements.sort(key=lambda x: x[1])
    best_candidate, best_nrmse, best_model, best_names = improvements[0]
    if best_nrmse < best_val_err:
        selected.append(best_candidate)
        best_val_err = best_nrmse
        history.append((selected.copy(), best_val_err))
        print(f"Step {step+1}: added {best_candidate}, val NRMSE = {best_val_err:.4f}")
    else:
        print("No candidate improved validation error. Stopping.")
        break

print("\nSelected edges (input,lag):", selected)
print("History (selected, val_nrmse):", history)

# -----------------------------
# 4) Final fit on train+val and evaluate on test
# -----------------------------
final_specs = [(inp, lag, n_terms_per_input) for (inp, lag) in selected]
X_all, y_all, names = build_regressors(df, final_specs, 'plant_y', n_arx=n_arx)
# build valid mask to slice train+val vs test
valid_mask, offset = True, 0
# recompute valid mask robustly
def compute_valid_mask(df, specs, n_arx=2):
    yfull = df['plant_y'].to_numpy()
    N = len(yfull)
    total_shift = 0
    for (_, lag, _) in specs:
        total_shift = max(total_shift, lag + n_terms_per_input - 1)
    total_shift = max(total_shift, n_arx)
    mask = np.ones(N, dtype=bool)
    mask[:total_shift] = False
    return mask, total_shift

mask, offset = compute_valid_mask(df, final_specs, n_arx=n_arx)
idxs = np.where(mask)[0]
# split
trainval_mask = (idxs >= 0) & (idxs < n_train + n_val)
test_mask = (idxs >= n_train + n_val) & (idxs < T)
X_trainval = X_all[trainval_mask]; y_trainval = y_all[trainval_mask]
X_test = X_all[test_mask]; y_test = y_all[test_mask]

final_model = LinearRegression(fit_intercept=True)
final_model.fit(X_trainval, y_trainval)
y_pred_test = final_model.predict(X_test)
nrmse_test = math.sqrt(mean_squared_error(y_test, y_pred_test)) / (y_test.max() - y_test.min())
print(f"\nFinal test NRMSE (one-step) = {nrmse_test:.4f} on {X_test.shape[0]} samples")

# plot measured vs predicted on test segment (aligned in time)
test_times = idxs[test_mask]
plt.figure(figsize=(10,4))
plt.plot(test_times, y_test, label="measured")
plt.plot(test_times, y_pred_test, label="predicted", linestyle='--')
plt.xlabel("time (index)")
plt.ylabel("plant_y")
plt.title("Measured vs Predicted (test) — one-step ARX-ish model")
plt.legend()
plt.tight_layout()
plt.show()

# Print coefficients with feature names
print("\nFinal model coefficients:")
for name, coef in zip(names, final_model.coef_):
    print(f"{name:15s} : {coef:.4f}")

