import pandas as pd
import numpy as np

df = pd.read_csv(r'..\finalsim2.csv')
df.columns = [c.strip() for c in df.columns]

y = np.diff(df['23VA0001:LevelFeedSideWeir'])
# We want to see how y relates to the flows:
u_in = df['23UV0001_pf:MassFlow'].values[:-1]
u_out1 = df['23LV0001_pf:MassFlow'].values[:-1]
u_out2 = df['25ESV0001_pf:MassFlow'].values[:-1]

# Let's stack them into matrix
X = np.column_stack((u_in, u_out1, u_out2))

# Solve Ordinary Least Squares: Y = X * beta
# y = X * beta => beta_hat = pinv(X) * y
beta, residuals, rank, s = np.linalg.lstsq(X, y, rcond=None)
print("Coefficients for [In, Out1, Out2]:", beta)

# Let's see if providing an intercept helps
X2 = np.column_stack((u_in, u_out1, u_out2, np.ones(len(y))))
beta2, residuals2, rank2, s2 = np.linalg.lstsq(X2, y, rcond=None)
print("Coefficients with intercept:", beta2)
