# import osqp
# import numpy as np
# import scipy as sp
# from scipy import sparse
import json

# # Define P, q, A, l, u
# P = sparse.csc_matrix([[4, 1], [1, 2]])
# q = np.array([1, 1])
# A = sparse.csc_matrix([[1, 1], [1, 0], [0, 1]])
# l = np.array([1, 0, 0])
# u = np.array([1, 0.7, 0.7])

# # Create an OSQP object
# prob = osqp.OSQP()

# # Setup the problem
# prob.setup(P, q, A, l, u,max_iter = 1)

# # Solve the problem
# res = prob.solve()
# print(json.dumps({"solution": res.x.tolist(), "objective": res.info.obj_val}))

import osqp
import numpy as np
import scipy as sp
from scipy import sparse

# Generate problem data
sp.random.seed(1)
m = 3
n = 2
Ad = sparse.random(m, n, density=0.7, format='csc')
print(Ad.toarray())
b = np.random.randn(m)

# OSQP data
P = sparse.block_diag([sparse.csc_matrix((n, n)), sparse.eye(m)], format='csc')
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
# print(json.dumps({"solution": res.x.tolist(), "objective": res.info.obj_val}))