import os

class RowCountingProcessor:
  def __init__(self):
    self.output_folder = None
    self.values_counted = 0

  def set_output_folder(self, folder):
      self.output_folder = folder

  def scan(self, key, value):
      self.values_counted += 1

  def write_output(self):
      open(os.path.join(self.output_folder, "counted-rows"), "w").write(str(self.values_counted))

  def clear_state(self):
      self.values_counted = 0

Processor = RowCountingProcessor
