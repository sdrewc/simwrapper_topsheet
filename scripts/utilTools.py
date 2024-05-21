import re

def readEqvFile(eqvfile):
    """Reads the given eqvfile and returns
    distnames: distnum -> distname
            distToTaz: distnum -> list of taznums
            tazToDist: taznum  -> list of distnums
            numdists:  just the number of districts
    """
    f = open(eqvfile, "r")
    eqvtxt = f.read()
    f.close()

    distline_re = re.compile("DIST (\d+)=(\d+)( .+)?")
    lines = eqvtxt.split("\n")
    lineno = 0
    distnames = {}
    distToTaz = {}
    tazToDist = {}
    while lineno < len(lines):
        m = distline_re.search(lines[lineno])
        if m != None:
            # distnames[int(m.group(1))] = m.group(2)
            dist = int(m.group(1))
            taz = int(m.group(2))
            if dist not in distToTaz:
                distToTaz[dist] = []
            distToTaz[dist].append(taz)
            if taz not in tazToDist:
                tazToDist[taz] = []
            tazToDist[taz].append(dist)
            if m.group(3) != None:
                distnames[dist] = m.group(3).strip(" ")
        lineno = lineno + 1
    numdists = len(distnames)
    return (distnames, distToTaz, tazToDist, numdists)


class DataFrameToCustomHTML:
    def __init__(self, bold_rows=[], left_align_columns=[], bottom_line_rows=[]):
        self.bold_rows = bold_rows
        self.left_align_columns = left_align_columns
        self.bottom_line_rows = bottom_line_rows

    def generate_html(self, df, output_file, float=False):
        # Start with the table tag and styles
        html = "<table>\n"

        # Add styles
        html += """
<style>
.text-left {
    text-align: left !important;
}
.text-center {
    text-align: center !important;
}
.text-right {
    text-align: right !important;
}
.text-bot {
    vertical-align: bottom;
}
.padding-lr-10 {
    padding-left: 10px;
    padding-right: 10px;
}
.bold {
    font-weight: bold;
}
.bot-line {
    border-bottom: 1px dashed black;
}
</style>
"""

        # Create the header row
        html += '<thead>\n<tr class="bold text-bot padding-lr-10">\n'
        for i, col in enumerate(df.columns):
            align_class = "text-left" if i in self.left_align_columns else "text-center"
            html += f'<th class="{align_class}  padding-lr-10">{col}</th>\n'
        html += "</tr>\n</thead>\n"

        # Create the body of the table
        html += "<tbody>\n"
        for idx, row in df.iterrows():
            row_classes = []
            if idx in self.bold_rows:
                row_classes.append("bold")
            if idx in self.bottom_line_rows:
                row_classes.append("bot-line")
            class_attr = ' class="' + " ".join(row_classes) + '"' if row_classes else ""
            html += f"<tr{class_attr}>\n"
            for i, col in enumerate(df.columns):
                value = row[col]
                if not float:
                    formatted_value = (
                        f"{value:,.0f}" if type(value) in [int, float] else value
                    )

                else:
                    formatted_value = value
                align_class = (
                    "text-left" if i in self.left_align_columns else "text-right"
                )
                html += f'<td class="{align_class}">{formatted_value}</td>\n'
            html += "</tr>\n"
        html += "</tbody>\n"

        # Close the table tag
        html += "</table>\n"
        with open(output_file, "w") as f:
            f.write(html)
        return html


def modifyDistrictNameForMap(df, col):
    df.at[2, col] = "N.Beach/"
    df.at[3, col] = "Western"
    df.at[4, col] = "Mission/"
    df.at[5, col] = "Noe/"
    df.at[6, col] = "Marina/"
    df.at[9, col] = "Outer"
    df.at[10, col] = "Hill"
    return df
