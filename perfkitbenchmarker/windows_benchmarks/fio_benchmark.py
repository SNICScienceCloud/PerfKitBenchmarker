# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_benchmarks import fio_benchmark as linux_fio
from perfkitbenchmarker.windows_packages import fio


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'fio'
BENCHMARK_CONFIG = linux_fio.BENCHMARK_CONFIG


def FillDevice(vm, disk, fill_size):
  """Fill the given disk on the given vm up to fill_size.

  Args:
    vm: a windows_virtual_machine.WindowsMixin object.
    disk: a disk.BaseDisk attached to the given vm.
    fill_size: amount of device to fill, in fio format.
  """

  command = (('%s --filename=%s --ioengine=libaio '
              '--name=fill-device --blocksize=512k --iodepth=64 '
              '--rw=write --direct=1 --size=%s') %
             (fio.FIO_PATH, disk.GetDevicePath(), fill_size))

  vm.RemoteCommand(command)


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.fio_target_mode != linux_fio.AGAINST_FILE_WITHOUT_FILL_MODE:
    disk_spec = config['vm_groups']['default']['disk_spec']
    for cloud in disk_spec:
      disk_spec[cloud]['mount_point'] = None
  return config


def Prepare(benchmark_spec):

  linux_fio.WarnOnBadFlags()

  vm = benchmark_spec.vms[0]
  logging.info('Fio prepare on %s', vm)
  vm.Install('fio')

  disk = vm.scratch_disks[0]

  if linux_fio.FillTarget():
    logging.info('Fill device %s on %s', disk.GetDevicePath(), vm)
    FillDevice(vm, disk, FLAGS.fio_fill_size)


def Run(benchmark_spec):
  vm = benchmark_spec.vms[0]
  logging.info('Fio run on %s', vm)

  return []


def Cleanup(benchmark_spec):
  vm = benchmark_spec.vms[0]
  logging.info('Fio cleanup on %s', vm)
