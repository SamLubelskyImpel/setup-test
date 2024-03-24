from csv import DictReader, DictWriter

class CsvFormatter:
    def __init__(self, dealer_id = None) -> None:
        self.dealer_id = dealer_id
        self.filtered_rows = []
        self.fieldnames = []

    def read_local_file(self, file_name):
        with open(file_name, "r", newline='') as file:
            dict_reader = DictReader(file, delimiter='|')
            if self.dealer_id:
                self.filtered_rows = [row for row in dict_reader if row["Site ID"] == self.dealer_id]
            else:
                self.filtered_rows = dict_reader
            self.fieldnames = dict_reader.fieldnames

    def write_csv_file(self, file_name):
        with open(file_name, "w", newline='') as file:
            dict_writer = DictWriter(file, fieldnames=self.fieldnames, delimiter='|')
            dict_writer.writeheader()
            dict_writer.writerows(self.filtered_rows)
