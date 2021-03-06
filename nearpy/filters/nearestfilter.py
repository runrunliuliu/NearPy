# -*- coding: utf-8 -*-

# Copyright (c) 2013 Ole Krause-Sparmann

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
import logging
from nearpy.filters.vectorfilter import VectorFilter

logger  = logging.getLogger('Engine') 

class NearestFilter(VectorFilter):
    """
    Sorts vectors with respect to distance and returns the N nearest.
    """

    def __init__(self, N):
        """
        Keeps the count threshold.
        """
        self.N = N

    def filter_vectors(self, input_list, indmap):
        """
        Returns subset of specified input list.
        """
        try:
            out  = []
            uniq = set() 
            # Return filtered (vector, data, distance )tuple list. Will fail
            # if input is list of (vector, data) tuples.
            # sorted_list = sorted(input_list, key=lambda x: x[2])
            # sorted_list = sorted(input_list, key=lambda x: x[2])
            # return sorted_list[:self.N]
            input_list.sort(key=lambda x: x[2])

            cnt = 0
            for k in input_list:
                if cnt == self.N:
                    break
                ind = int(k[1])
                if ind not in indmap:
                    logger.error('missing:{}'.format(k[1]))
                    continue
                key = indmap[ind]
                if key not in uniq:
                    out.append(k)
                    uniq.add(key)
                    cnt = cnt + 1
                else:
                    logger.debug('Duplicate:{}'.format(key))
            return out
        except:
            # Otherwise just return input list
            return input_list
