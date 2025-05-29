from collections import defaultdict


class OGMATarget:
    def __init__(self):
        self.lr_plan = defaultdict(self.LRPlan)

    class LRPlan:
        def __init__(self):
            self.ndt = defaultdict(self.NDT)

        class NDT:
            def __init__(self):
                self.bec_zone = defaultdict(self.Zone)

            class Zone:
                def __init__(self):
                    self.bio_opt = defaultdict(self.BEO)

                class BEO:
                    def __init__(self):
                        self.mature = self.AgeTarget()
                        self.old = self.AgeTarget()

                    class AgeTarget:
                        def __init__(self):
                            self.age = None
                            self.target = None
