from unittest import TestCase

from ftg import coi


class ModelTestCase(TestCase):
    maxDiff = None

    def test_coi_split_granular(self):
        # testing the different cases of author names
        names = (
            "Paula Theda Anderson",
            "Paula Anderson",
            "PTA",
            "PT A",
            "P.T.A.",
            "P. T. A.",
            "P-TA",
            "P.-T.A.",
            "P.-T. A.",
            "PT Anderson",
            "P.T. Anderson",
            "P. T. Anderson",
            "P Anderson",
            "P. Anderson",
            "Paula T Anderson",
            "Paula T. Anderson",
        )

        author = ("Paula Theda", "Anderson")
        tests = list(coi._get_author_tests(author))

        for name in names:
            stmt = f"Competing interest: {name} has one."
            res = coi._test_sentence(stmt, tests)
            self.assertTrue(res)
            # name at the beginning of sentence
            stmt = f"{name} has one."
            self.assertTrue(coi._test_sentence(stmt, tests))

    def test_coi_split_all_authors_nothing(self):
        statement = "The authors declare nothing."
        authors = (("Paula Theda", "Anderson"), ("Lisa", "Smith"))
        res = coi.split_coi(statement, authors)
        self.assertEqual(
            res[("Paula Theda", "Anderson")],
            [
                "The authors declare nothing.",
            ],
        )
        self.assertEqual(
            res[("Lisa", "Smith")],
            [
                "The authors declare nothing.",
            ],
        )
        statement = "nothing declared"
        authors = (("Paula Theda", "Anderson"), ("Lisa", "Smith"))
        res = coi.split_coi(statement, authors)
        self.assertEqual(
            res[("Paula Theda", "Anderson")],
            [
                "nothing declared",
            ],
        )
        self.assertEqual(
            res[("Lisa", "Smith")],
            [
                "nothing declared",
            ],
        )

    def test_coi_split_next_sentence(self):
        statement = """Paula Anderson has a conflict.
                       She works at Bayer.
                       All remaining authors have no conflict.
                       And here a last sentence."""

        authors = (("Paula Theda", "Anderson"), ("Lisa", "Smith"))
        res = coi.split_coi(statement, authors)
        self.assertEqual(
            res[("Paula Theda", "Anderson")],
            [
                "Paula Anderson has a conflict.",
                "She works at Bayer.",
                "And here a last sentence.",
            ],
        )
        self.assertEqual(
            res[("Lisa", "Smith")],
            ["All remaining authors have no conflict.", "And here a last sentence."],
        )

    def test_coi_split_next_sentence_multiple_authors(self):
        statement = """Paula Anderson and Lisa Smith have a conflict.
                       They work at Bayer.
                       Lisa Smith has another conflict as well.
                       All remaining authors have no conflict.
                       And here a last sentence."""

        authors = (("Paula Theda", "Anderson"), ("Lisa", "Smith"), ("Alice", "Mueller"))
        res = coi.split_coi(statement, authors)
        self.assertDictEqual(
            {
                ("Paula Theda", "Anderson"): [
                    "Paula Anderson and Lisa Smith have a conflict.",
                    "They work at Bayer.",
                    "And here a last sentence.",
                ],
                ("Lisa", "Smith"): [
                    "Paula Anderson and Lisa Smith have a conflict.",
                    "They work at Bayer.",
                    "Lisa Smith has another conflict as well.",
                    "And here a last sentence.",
                ],
                ("Alice", "Mueller"): [
                    "All remaining authors have no conflict.",
                    "And here a last sentence.",
                ],
            },
            res,
        )

        statement = """Paula Anderson and Lisa Smith have a conflict.
                       They work at Bayer.
                       Lisa Smith has another conflict as well.
                       And so has Paula Anderson another conflict.
                       And here a last sentence."""

        authors = (("Paula Theda", "Anderson"), ("Lisa", "Smith"))
        res = coi.split_coi(statement, authors)
        self.assertDictEqual(
            {
                ("Paula Theda", "Anderson"): [
                    "Paula Anderson and Lisa Smith have a conflict.",
                    "They work at Bayer.",
                    "And so has Paula Anderson another conflict.",
                    "And here a last sentence.",
                ],
                ("Lisa", "Smith"): [
                    "Paula Anderson and Lisa Smith have a conflict.",
                    "They work at Bayer.",
                    "Lisa Smith has another conflict as well.",
                    "And here a last sentence.",
                ],
            },
            res,
        )

    def test_coi_split_remaining_authors(self):
        statement = """Competing Interests: MS, MR, KL, MAE,
            KMS, DCW, AD, and DBK are employees of Verseon Corporation. EDC
            discloses a financial interest in Verseon Corporation and funding
            through NIH grants HL049413, HL073813, and HL112303. """

        authors = [
            ("Mohanram", "Sivaraja"),
            ("Nicola", "Pozzi"),
            ("Matthew", "Rienzo"),
            ("Kenneth", "Lin"),
            ("Timothy P.", "Shiau"),
            ("Daniel M.", "Clemens"),
            ("Lev", "Igoudin"),
            ("Piotr", "Zalicki"),
            ("Stephanie S.", "Chang"),
            ("M. Angels", "Estiarte"),
            ("Kevin M.", "Short"),
            ("David C.", "Williams"),
            ("Anirban", "Datta"),
            ("Enrico", "Di Cera"),
            ("David B.", "Kita"),
        ]

        phrases = (
            "All remaining authors declare a conflict.",
            "All remaining co-authors declare a conflict.",
            "The remaining authors declare a conflict.",
            "The remaining co-authors declare a conflict.",
            "All other authors declare a conflict.",
            "All other co-authors declare a conflict.",
            "The other authors declare a conflict.",
            "The other co-authors declare a conflict.",
        )

        for phrase in phrases:
            stmt = statement + phrase
            res = coi.split_coi(stmt, authors)
            remaining_authors = [
                author for author, sentences in res.items() if phrase in sentences
            ]
            self.assertSetEqual(
                set(remaining_authors),
                set(
                    [
                        ("Nicola", "Pozzi"),
                        ("Timothy P.", "Shiau"),
                        ("Daniel M.", "Clemens"),
                        ("Lev", "Igoudin"),
                        ("Piotr", "Zalicki"),
                        ("Stephanie S.", "Chang"),
                    ]
                ),
            )

    def test_coi_split(self):
        statement = """Competing Interests: MS, MR, KL, TPS, DMC, LI, SC, PZ, MAE,
            KMS, DCW, AD, and DBK are employees of Verseon Corporation. EDC
            discloses a financial interest in Verseon Corporation and funding
            through NIH grants HL049413, HL073813, and HL112303. NP discloses a
            financial interest in Hemadvance, LLC and funding through AHA grant
            AHA15SDG25550094. KMS and DCW are inventors on a patent application
            (WO/2014/149139) that includes Compound 1 and has local applications
            pending in numerous jurisdictions worldwide. This does not alter our
            adherence to PLOS ONE policies on sharing data and materials."""

        authors = [
            ("Mohanram", "Sivaraja"),
            ("Nicola", "Pozzi"),
            ("Matthew", "Rienzo"),
            ("Kenneth", "Lin"),
            ("Timothy P.", "Shiau"),
            ("Daniel M.", "Clemens"),
            ("Lev", "Igoudin"),
            ("Piotr", "Zalicki"),
            ("Stephanie S.", "Chang"),
            ("M. Angels", "Estiarte"),
            ("Kevin M.", "Short"),
            ("David C.", "Williams"),
            ("Anirban", "Datta"),
            ("Enrico", "Di Cera"),
            ("David B.", "Kita"),
        ]

        res = coi.split_coi(statement, authors)

        self.assertDictEqual(
            {
                ("Mohanram", "Sivaraja"): [
                    "Competing Interests: MS, MR, KL, TPS, DMC, LI, SC, PZ, MAE, KMS, DCW, AD, and DBK are employees of Verseon Corporation.",
                    "This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                ],
                ("Matthew", "Rienzo"): [
                    "Competing Interests: MS, MR, KL, TPS, DMC, LI, SC, PZ, MAE, KMS, DCW, AD, and DBK are employees of Verseon Corporation.",
                    "This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                ],
                ("Kenneth", "Lin"): [
                    "Competing Interests: MS, MR, KL, TPS, DMC, LI, SC, PZ, MAE, KMS, DCW, AD, and DBK are employees of Verseon Corporation.",
                    "This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                ],
                ("Timothy P.", "Shiau"): [
                    "Competing Interests: MS, MR, KL, TPS, DMC, LI, SC, PZ, MAE, KMS, DCW, AD, and DBK are employees of Verseon Corporation.",
                    "This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                ],
                ("Daniel M.", "Clemens"): [
                    "Competing Interests: MS, MR, KL, TPS, DMC, LI, SC, PZ, MAE, KMS, DCW, AD, and DBK are employees of Verseon Corporation.",
                    "This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                ],
                ("Lev", "Igoudin"): [
                    "Competing Interests: MS, MR, KL, TPS, DMC, LI, SC, PZ, MAE, KMS, DCW, AD, and DBK are employees of Verseon Corporation.",
                    "This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                ],
                ("Piotr", "Zalicki"): [
                    "Competing Interests: MS, MR, KL, TPS, DMC, LI, SC, PZ, MAE, KMS, DCW, AD, and DBK are employees of Verseon Corporation.",
                    "This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                ],
                ("Stephanie S.", "Chang"): [
                    "Competing Interests: MS, MR, KL, TPS, DMC, LI, SC, PZ, MAE, KMS, DCW, AD, and DBK are employees of Verseon Corporation.",
                    "This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                ],
                ("M. Angels", "Estiarte"): [
                    "Competing Interests: MS, MR, KL, TPS, DMC, LI, SC, PZ, MAE, KMS, DCW, AD, and DBK are employees of Verseon Corporation.",
                    "This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                ],
                ("Kevin M.", "Short"): [
                    "Competing Interests: MS, MR, KL, TPS, DMC, LI, SC, PZ, MAE, KMS, DCW, AD, and DBK are employees of Verseon Corporation.",
                    "KMS and DCW are inventors on a patent application (WO/2014/149139) that includes Compound 1 and has local applications pending in numerous jurisdictions worldwide.",
                    "This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                ],
                ("David C.", "Williams"): [
                    "Competing Interests: MS, MR, KL, TPS, DMC, LI, SC, PZ, MAE, KMS, DCW, AD, and DBK are employees of Verseon Corporation.",
                    "KMS and DCW are inventors on a patent application (WO/2014/149139) that includes Compound 1 and has local applications pending in numerous jurisdictions worldwide.",
                    "This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                ],
                ("Anirban", "Datta"): [
                    "Competing Interests: MS, MR, KL, TPS, DMC, LI, SC, PZ, MAE, KMS, DCW, AD, and DBK are employees of Verseon Corporation.",
                    "This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                ],
                ("David B.", "Kita"): [
                    "Competing Interests: MS, MR, KL, TPS, DMC, LI, SC, PZ, MAE, KMS, DCW, AD, and DBK are employees of Verseon Corporation.",
                    "This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                ],
                ("Enrico", "Di Cera"): [
                    "EDC discloses a financial interest in Verseon Corporation and funding through NIH grants HL049413, HL073813, and HL112303.",
                    "This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                ],
                ("Nicola", "Pozzi"): [
                    "NP discloses a financial interest in Hemadvance, LLC and funding through AHA grant AHA15SDG25550094.",
                    "This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                ],
            },
            res,
        )

    def test_flag_coi(self):
        """FIXME this currently tests only the invocation, not the flagging itself (aka if the results makes sense)"""
        with open("./testdata/medrxiv_cois.txt") as f:
            cois = f.readlines()
        for text in cois:
            coi.flag_coi(text)
            coi.flag_coi_rtrans(text)
            coi.flag_coi_hristio(text)
