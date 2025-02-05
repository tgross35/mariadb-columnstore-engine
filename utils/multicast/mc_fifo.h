/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#pragma once

#include "threads.h"
#include "produconsum.h"

typedef struct fifo
{
  unsigned char* dataBuffer;
  unsigned int dataBufSize;

  produconsum_t freeMemQueue; /* queue for free memory */
  produconsum_t data;         /* queue for received data or data received
                               * from disk */

  pthread_t thread;
} * fifo_t;
