# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

def split(big_list, split_size):
        split_list = []
        len_big_list = len(big_list)
        current = 0
        while current + split_size < len_big_list:
            split_list.append(big_list[current:current+split_size])
            current += split_size
        if current < len_big_list:
            split_list.append(big_list[current:])
        return split_list
