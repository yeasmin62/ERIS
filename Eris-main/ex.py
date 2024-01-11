import osqp
import numpy as np
import scipy as sp
from scipy import sparse
import json

# Generate problem data
sp.random.seed(1)
m = 3
n = 2
Ad = sparse.random(m, n, density=0.7, format='csc')
b = np.random.randn(m)

# OSQP data
P = sparse.block_diag([sparse.csc_matrix((n, n)), sparse.eye(m)], format='csc')
print(P.toarray())
# print
q = np.zeros(n+m)
A = sparse.bmat([[Ad,            -sparse.eye(m)],
                 [sparse.eye(n),  None]], format='csc')
print(A.toarray())
l = np.hstack([b, np.zeros(n)])
u = np.hstack([b, np.ones(n)])

# Create an OSQP object
prob = osqp.OSQP()

# Setup workspace
prob.setup(P, q, A, l, u)

# Solve problem
res = prob.solve()
print(json.dumps({"solution": res.x.tolist(), "objective": res.info.obj_val}))


