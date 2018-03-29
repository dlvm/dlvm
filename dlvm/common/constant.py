#!/usr/bin/env python

import os

lc_path = os.environ.get('DLVM_CONF', '/etc/dlvm')
dpv_search_overhead = 2
max_thin_id = 2 ** 24
max_thin_id_retry = 3
