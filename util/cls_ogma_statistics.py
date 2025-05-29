from collections import defaultdict


class OGMAStatistics:
    def __init__(self):
        self.nat_disturbance = defaultdict(self.NatDisturbance)
        self.lr_plan = ''
        self.lu_number = ''
        self.area = 0
        self.park_name = None
        self.park_number = None

    def total(self):
        for val in self.nat_disturbance:
            self.nat_disturbance[val].total()
            self.area += self.nat_disturbance[val].area
        return self.area

    class NatDisturbance:
        def __init__(self):
            self.zone = defaultdict(self.Zone)
            self.area = 0
            self.ac_count = 0
            self.bio_count = 0

        def total(self):
            for val in self.zone:
                self.zone[val].total()
                self.area += self.zone[val].area
                self.ac_count += self.zone[val].ac_count
                self.bio_count += self.zone[val].bio_count
            return self.area

        class Zone:
            def __init__(self):
                self.bio_opt = defaultdict(self.BioOpt)
                self.area = 0
                self.ac_count = 0
                self.bio_count = 0

            def total(self):
                for val in self.bio_opt:
                    self.bio_opt[val].total()
                    self.area += self.bio_opt[val].area
                    self.ac_count += self.bio_opt[val].ac_count
                    self.bio_count += 1
                return self.area

            class BioOpt:
                def __init__(self):
                    self.status = defaultdict(self.Status)
                    self.area = 0
                    self.ac_count = 0

                def total(self):
                    for val in self.status:
                        self.status[val].total()
                        self.area += self.status[val].area
                        self.ac_count += self.status[val].ac_count
                    return self.area

                class Status:
                    def __init__(self):
                        self.age_class = defaultdict(self.AgeClass)
                        self.area = 0
                        self.ac_count = 0

                    def total(self):
                        for val in self.age_class:
                            self.age_class[val].total()
                            self.area += self.age_class[val].area
                            self.ac_count += 1
                        return self.area

                    class AgeClass:
                        def __init__(self):
                            self.ac_type = ''
                            self.area = 0
                            self.op_areas = defaultdict(self.OperatingArea)

                        def total(self):
                            for val in self.op_areas:
                                self.op_areas[val].total()
                                self.area += self.op_areas[val].area

                            return self.area

                        class OperatingArea:
                            def __init__(self):
                                self.land_type = defaultdict(self.LandType)
                                self.conn_area = 0
                                self.area = 0

                            def total(self):
                                for val in self.land_type:
                                    self.land_type[val].total()
                                    self.area += self.land_type[val].area

                            class LandType:
                                def __init__(self):
                                    self.operable = defaultdict(self.Operable)
                                    self.area = 0

                                def total(self):
                                    for val in self.operable:
                                        self.area += self.operable[val].area
                                    return self.area

                                class Operable:
                                    def __init__(self):
                                        self.area = 0
