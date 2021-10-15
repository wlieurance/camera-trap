import tkinter as tk
import sqlite3 as sqlite
import pandas as pd
import os
import re
from PIL import ImageTk, Image
from tkinter.font import Font
from datetime import datetime
from dateutil.parser import parse

COLORS = [
    {'score': 1, 'color': 'red4'},
    {'score': 2, 'color': 'red'},
    {'score': 3, 'color': 'orange'},
    {'score': 4, 'color': 'yellow'},
    {'score': 5, 'color': 'lime green'},
    {'score': 6, 'color': 'turquoise'},
    {'score': 7, 'color': 'sky blue'},
    {'score': 8, 'color': 'blue'},
    {'score': 9, 'color': 'purple'}
]


class PhotoViewer(tk.Tk):
    def __init__(self, dbpath, photo_dir, title="Photo Viewer"):
        super().__init__()
        self.title(title)

        # connect to db
        self.dbpath = dbpath
        self.photo_dir = photo_dir
        self.con = sqlite.connect(self.dbpath)
        self.con.row_factory = sqlite.Row

        # other vars
        self.rated_seqs = pd.DataFrame()
        self.filtered_seqs = pd.DataFrame()
        self.photos = pd.DataFrame()
        self.filtered_photos = pd.DataFrame()
        self.ratings = pd.DataFrame()
        self.filtered_ratings = pd.DataFrame()
        self.rating_hashes = pd.DataFrame()
        self.rating_seqs = pd.DataFrame()
        self.current_seq = None
        self.current_photos = None
        self.current_ratings = None
        self.displayed_photo = None
        self.os_path = None
        self.photo_no = 0
        self.seq_no = 0
        self.last_seq_filter = {'min_dt': None, 'max_dt': None, 'site_name': []}
        self.last_rating_filter = {'score_low': None, 'score_high': None}

        # Add a canvas
        self.canvas = tk.Canvas(self, width=1000, height=1000, borderwidth=0, highlightthickness=0)

        # image init
        self.orig_w, self.orig_h = None, None
        self.w, self.h = None, None
        self.w_ratio, self.h_ratio = 1, 1

        # Create a vertical scrollbar linked to the canvas.
        self.vsbar = tk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.vsbar.set)

        # Create a horizontal scrollbar linked to the canvas.
        self.hsbar = tk.Scrollbar(self, orient="horizontal", command=self.canvas.xview)
        self.canvas.configure(xscrollcommand=self.hsbar.set)

        # next/prev buttons
        self.bottom_left_frame = tk.Frame()
        self.bottom_right_frame = tk.Frame()

        self.next_seq = tk.Button(self.bottom_right_frame)
        self.next_seq["text"] = ">>"
        self.next_seq["command"] = self._next_seq

        self.prev_seq = tk.Button(self.bottom_left_frame)
        self.prev_seq["text"] = "<<"
        self.prev_seq["command"] = self._prev_seq

        self.next_photo = tk.Button(self.bottom_right_frame)
        self.next_photo["text"] = ">"
        self.next_photo["command"] = self._next_image

        self.prev_photo = tk.Button(self.bottom_left_frame)
        self.prev_photo["text"] = "<"
        self.prev_photo["command"] = self._prev_image

        # labels/entries
        self.info_frame = tk.LabelFrame(self, text="Photo Information", labelanchor='n')

        self.seq_lbl = tk.Label(self.info_frame, text="Sequence Id: ", justify="right")
        self.seq_str = tk.StringVar()
        self.seq_entry = tk.Entry(self.info_frame, textvariable=self.seq_str, fg="black", bg="white", bd=0,
                                  state="readonly")

        self.sno_lbl = tk.Label(self.info_frame, text="Sequence #: ", justify="right")
        self.sno_str = tk.StringVar()
        self.sno_entry = tk.Entry(self.info_frame, textvariable=self.sno_str, fg="black", bg="white", bd=0,
                                  state="readonly")

        self.pno_lbl = tk.Label(self.info_frame, text="Photo #: ", justify="right")
        self.pno_str = tk.StringVar()
        self.pno_entry = tk.Entry(self.info_frame, textvariable=self.pno_str, fg="black", bg="white", bd=0,
                                  state="readonly")

        self.path_lbl = tk.Label(self.info_frame, text="Local path: ", justify="right")
        self.path_str = tk.StringVar()
        self.path_entry = tk.Entry(self.info_frame, text=self.path_str, fg="black", bg="white", bd=0,
                                   state="readonly")

        self.dbpath_lbl = tk.Label(self.info_frame, text="DB path: ", justify="right")
        self.dbpath_str = tk.StringVar()
        self.dbpath_entry = tk.Entry(self.info_frame, text=self.dbpath_str, fg="black", bg="white", bd=0,
                                     state="readonly")

        self.hash_lbl = tk.Label(self.info_frame, text="MD5 hash: ", justify="right")
        self.hash_str = tk.StringVar()
        self.hash_entry = tk.Entry(self.info_frame, text=self.hash_str, fg="black", bg="white", bd=0,
                                   state="readonly")

        # filters
        # frames / labels
        self.filter_frame = tk.LabelFrame(self, text="Filters", labelanchor='n')
        self.date_start_lbl = tk.Label(self.filter_frame, text="Start")
        self.date_end_lbl = tk.Label(self.filter_frame, text="End")
        self.seq_date_lbl = tk.Label(self.filter_frame, text="Sequence Dates (inclusive)")
        self.site_name_lbl = tk.Label(self.filter_frame, text="Site Name")
        self.score_lbl = tk.Label(self.filter_frame, text="Score (inclusive)")
        # dates
        self.score_lower_lbl = tk.Label(self.filter_frame, text="Low")
        self.score_higher_lbl = tk.Label(self.filter_frame, text="High")
        date_lower_validate = (self.register(self._date_lower_change), '%d', '%i', '%s', '%S', '%P', '%V')
        date_higher_validate = (self.register(self._date_higher_change), '%d', '%i', '%s', '%S', '%P', '%V')
        self.date_lower_str = tk.StringVar()
        self.date_lower_str.set("yyyy-mm-dd")
        self.date_higher_str = tk.StringVar()
        self.date_higher_str.set("yyyy-mm-dd")
        self.date_lower = tk.Entry(self.filter_frame, width=13, textvariable=self.date_lower_str,
                                   validate="key", validatecommand=date_lower_validate, fg="gray50")
        self.date_higher = tk.Entry(self.filter_frame, width=13, textvariable=self.date_higher_str,
                                    validate="key", validatecommand=date_higher_validate, fg="gray50")
        # site_name
        self.site_name_str = tk.StringVar()
        self.site_name = tk.Listbox(self.filter_frame, height=5, selectmode=tk.MULTIPLE,
                                    listvariable=self.site_name_str)
        # score
        score_validate = (self.register(self._score_change), '%d', '%i', '%s', '%S', '%P', '%V')
        self.score_lower_str = tk.StringVar()
        self.score_lower_entry = tk.Entry(self.filter_frame, width=5, textvariable=self.score_lower_str, validate="key",
                                          validatecommand=score_validate)
        self.score_higher_str = tk.StringVar()
        self.score_higher_entry = tk.Entry(self.filter_frame, width=5, textvariable=self.score_higher_str,
                                           validate="key", validatecommand=score_validate)
        # filter buttons
        self.go_btn = tk.Button(self.filter_frame, text="Filter", padx=2, pady=2, command=self._set_canvas_focus)
        self.clear_btn = tk.Button(self.filter_frame, text="Clear filter", padx=2, pady=2,
                                   command=self._clear_filters)

        # main grid
        self.filter_frame.grid(row=0, column=0, sticky="w", columnspan=3)
        self.canvas.grid(row=1, column=0, sticky="nsew", columnspan=3)
        self.vsbar.grid(row=1, column=3, sticky="ns")
        self.hsbar.grid(row=2, column=0, sticky="ew", columnspan=3)
        self.bottom_left_frame.grid(row=3, column=0, sticky="ew")
        self.bottom_right_frame.grid(row=3, column=2, sticky="ew")
        self.info_frame.grid(row=4, column=0, sticky="ew", columnspan=3)

        # bottom_left_frame grid
        self.prev_seq.grid(row=0, column=0, sticky="w")
        self.prev_photo.grid(row=0, column=1, sticky="w")

        # bottom_right_frame grid
        self.next_photo.grid(row=0, column=0, sticky="e")
        self.next_seq.grid(row=0, column=1, sticky="e")

        # info_frame grid
        self.seq_lbl.grid(row=0, column=0, sticky="e")
        self.seq_entry.grid(row=0, column=1, sticky="w")
        self.sno_lbl.grid(row=1, column=0, sticky="e")
        self.sno_entry.grid(row=1, column=1, sticky="w")
        self.pno_lbl.grid(row=0, column=2, sticky="e")
        self.pno_entry.grid(row=0, column=3, sticky="w")
        self.path_lbl.grid(row=1, column=2, sticky="e")
        self.path_entry.grid(row=1, column=3, sticky="w")
        self.dbpath_lbl.grid(row=2, column=2, sticky="e")
        self.dbpath_entry.grid(row=2, column=3, sticky="w")
        self.hash_lbl.grid(row=3, column=2, sticky="e")
        self.hash_entry.grid(row=3, column=3, sticky="w")

        # filter_frame grid
        self.seq_date_lbl.grid(row=0, column=0, columnspan=2)
        self.site_name_lbl.grid(row=0, column=2)
        self.score_lbl.grid(row=0, column=3)
        self.date_start_lbl.grid(row=1, column=0)
        self.date_end_lbl.grid(row=2, column=0)
        self.date_lower.grid(row=1, column=1)
        self.date_higher.grid(row=2, column=1)
        self.site_name.grid(row=1, column=2, rowspan=2)
        self.score_lower_lbl.grid(row=1, column=3)
        self.score_higher_lbl.grid(row=2, column=3)
        self.score_lower_entry.grid(row=1, column=4)
        self.score_higher_entry.grid(row=2, column=4)
        self.go_btn.grid(row=0, column=5, sticky="e", padx=5)
        self.clear_btn.grid(row=0, column=6, sticky="e", padx=5)

        # widget resize settings
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        # bindings
        self.bind('<Enter>', self._on_app_bound)
        self.bind('<Leave>', self._off_app_unbound)
        self._bind_arrows()
        self.date_lower.bind('<FocusIn>', self._date_lower_focusin)
        self.date_lower.bind('<FocusOut>', self._date_lower_focusout)
        self.date_higher.bind('<FocusIn>', self._date_higher_focusin)
        self.date_higher.bind('<FocusOut>', self._date_higher_focusout)
        self.site_name.bind('<FocusOut>', self._filter_seqs)
        self.score_lower_entry.bind('<FocusOut>', self._filter_ratings)
        self.score_higher_entry.bind('<FocusOut>', self._filter_ratings)

        # read from db
        self.get_seqs()
        self.get_photos()
        self.get_ratings()

        # get current
        self.get_current_photos()
        # self.get_current_ratings()

        self._refresh_img()
        # self.canvas.configure(scrollregion=self.bbox)
        self.canvas.configure(scrollregion=(0, 0, self.w, self.h))

    def _set_canvas_focus(self):
        self.canvas.focus_set()

    def _clear_filters(self):
        self.date_lower_str.set("yyyy-mm-dd")
        self.date_lower.config(fg="gray50")
        self.date_higher_str.set("yyyy-mm-dd")
        self.date_higher.config(fg="gray50")
        self.score_lower_str.set("")
        self.score_higher_str.set("")
        self.site_name.selection_clear(0, 'end')
        self.filtered_seqs = self.rated_seqs.copy()
        self.filtered_photos = self.photos.copy()
        self.filtered_ratings = self.ratings.copy()
        self.seq_no = 0
        self.photo_no = 0
        self.current_seq = self.filtered_seqs.seq_id[self.seq_no]
        self.get_current_photos()
        self.get_sites()
        self._refresh_img()

    def _bind_arrows(self):
        self.bind('<Left>', self._prev_image)
        self.bind('<Right>', self._next_image)
        self.bind('<Up>', self._prev_seq)
        self.bind('<Down>', self._next_seq)

    def _unbind_arrows(self):
        self.unbind('<Left>')
        self.unbind('<Right>')
        self.unbind('<Up>')
        self.unbind('<Down>')

    def _on_app_bound(self, event=None):
        self.canvas.bind('<MouseWheel>', self._scroll_y)
        self.canvas.bind('<Shift-MouseWheel>', self._scroll_x)
        self.canvas.bind('<Control-MouseWheel>', self._img_resize)
        self.bind('a', self._scroll_w)
        self.bind('d', self._scroll_e)
        self.bind('w', self._scroll_n)
        self.bind('s', self._scroll_s)

    def _off_app_unbound(self, event=None):
        self.canvas.unbind('<MouseWheel>')
        self.canvas.unbind('<Shift-MouseWheel>')
        self.canvas.unbind('<Control-MouseWheel>')
        self.unbind('a')
        self.unbind('d')
        self.unbind('w')
        self.unbind('s')

    def _scroll_y(self, event):
        # On Windows, you bind to <MouseWheel> and you need to divide event.delta by 120 (or some other factor
        # depending on how fast you want the scroll)
        # On OSX, you bind to <MouseWheel> and you need to use event.delta without modification
        # On X11 systems you need to bind to <Button-4> and <Button-5>, and you need to divide event.delta by 120
        # (or some other factor depending on how fast you want to scroll)
        self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def _scroll_x(self, event):
        self.canvas.xview_scroll(int(-1 * (event.delta / 120)), "units")

    def _scroll_n(self, event=None):
        # print(event)
        self.canvas.yview_scroll(-1, "units")

    def _scroll_s(self, event=None):
        # print(event)
        self.canvas.yview_scroll(1, "units")

    def _scroll_e(self, event=None):
        # print(event)
        self.canvas.xview_scroll(1, "units")

    def _scroll_w(self, event=None):
        # print(event)
        self.canvas.xview_scroll(-1, "units")

    def _img_resize(self, event):
        scale = 1 + event.delta/1200  # event delta will be 120 or -120
        self.w = int(round(self.w * scale, 0))
        self.h = int(round(self.h * scale, 0))
        self.w_ratio = self.w / self.orig_w
        self.h_ratio = self.h / self.orig_h
        self.canvas.configure(height=self.h, width=self.w)
        self.canvas.configure(scrollregion=(0, 0, self.w, self.h))
        temp_img = self.img.resize((self.w, self.h))
        self.tk_img = ImageTk.PhotoImage(temp_img)
        self.canvas.delete("all")
        self.canvas.create_image(0, 0, anchor="nw", image=self.tk_img)
        self._draw_ratings()

    def get_seqs(self):
        print("getting rated sequences from db...")
        # will also work:
        # pd.read_sql_query(..., dtype="string")\
        #   .assign(max_dt=lambda y: y.max_dt.map(lambda x: datetime.fromisoformat(x)))
        self.rated_seqs = pd.read_sql_query(sql="""
        WITH rated_seqs AS (
        SELECT seq_id, group_concat(scorer_name, ', ') scorers 
          FROM condition_seqs 
         GROUP BY seq_id
        )
         
        SELECT a.*, b.site_name, b.camera_id, b.id, b.seq, b.min_dt, b.max_dt
          FROM rated_seqs a
          LEFT JOIN sequence b ON a.seq_id = b.seq_id
         ORDER BY site_name, camera_id, min_dt;
        """, con=self.con, parse_dates=['min_dt', 'max_dt'])

        self.filtered_seqs = self.rated_seqs.copy()
        self.current_seq = self.filtered_seqs.seq_id[self.seq_no]
        print("current seq_id:", self.current_seq)

    def get_photos(self):
        print("getting rated photos from db...")
        self.photos = pd.read_sql_query(sql="""
        WITH rated_seqs AS (
        SELECT seq_id, group_concat(scorer_name, ', ') scorers 
          FROM condition_seqs 
         GROUP BY seq_id
        )
         
        SELECT a.*, b.md5hash, b.id, b.cnt, b.classifier, b.coords,
               c.path, c.site_name, c.camera_id, c.dt_orig 
          FROM rated_seqs a
          LEFT JOIN animal b ON a.seq_id = b.seq_id
          LEFT JOIN photo c ON b.md5hash = c.md5hash
         ORDER BY c.site_name, c.camera_id, c.dt_orig;
        """, con=self.con, parse_dates=['dt_orig'])
        self.filtered_photos = self.photos.copy()
        self.get_sites()

    def get_ratings(self):
        print("getting ratings from db...")
        self.ratings = pd.read_sql_query(sql="""
        SELECT a.seq_id, a.scorer_name, a.scores,
               b.md5hash, b.rating, b.score_dt, b.bbox_x1,
               b.bbox_y1, b.bbox_x2, b.bbox_y2
          FROM condition_seqs a
          LEFT JOIN condition b ON a.seq_id = b.seq_id AND a.scorer_name = b.scorer_name
         ORDER BY a.seq_id, b.md5hash, a.scorer_name;
        """, con=self.con, parse_dates=['score_dt'])
        self.filtered_ratings = self.ratings.copy()

    def get_current_photos(self):
        print("getting current photos...")
        if self.filtered_seqs.shape[0] > 0:
            self.current_photos = self.filtered_photos.query('seq_id in @self.current_seq').sort_values(by=['dt_orig'])
            print("current seq:", self.current_seq)
            print("current photo no", self.current_photos.shape[0])
            if self.current_photos.shape[0] > 0:
                self.photo_no = 0
                self.displayed_photo = self.current_photos.iloc[self.photo_no]
            else:
                self.displayed_photo = pd.Series(data={'path': None, 'md5hash': None}, dtype="str")
                self.photo_no = -1
        else:
            self.current_photos = self.filtered_photos[0:0]  # empty the df
            self.displayed_photo = pd.Series(data={'path': None, 'md5hash': None}, dtype="str")
            self.photo_no = -1
        print("current path:", self.displayed_photo.path)
        print("number of photos: ", self.current_photos.shape[0])

    def get_current_ratings(self):
        print("getting current ratings")
        self.current_ratings = self.filtered_ratings.\
            query('md5hash == @self.displayed_photo.md5hash').\
            sort_values(by=['score_dt'])
        print(self.current_ratings)

    def get_sites(self):
        sites = self.rated_seqs.groupby('site_name', as_index=False)['site_name'].last().\
            sort_values(by=['site_name'])
        # self.site_name.delete(first=0, last=self.site_name.size())
        self.site_name_str.set(sites.site_name.tolist())
        # self.site_name.insert(tk.END, sites.site_name.tolist())

    def _prev_image(self, event=None):
        new_no = max(0, self.photo_no - 1)
        if new_no < self.photo_no:
            self.photo_no = new_no
            self.displayed_photo = self.current_photos.iloc[self.photo_no]
            self._refresh_img()

    def _next_image(self, event=None):
        new_no = min(self.current_photos.shape[0] - 1, self.photo_no + 1)
        if new_no > self.photo_no:
            self.photo_no = new_no
            self.displayed_photo = self.current_photos.iloc[self.photo_no]
            self._refresh_img()

    def _prev_seq(self, event=None):
        new_no = max(0, self.seq_no - 1)
        if new_no < self.seq_no:
            self.seq_no = new_no
            self.current_seq = self.filtered_seqs.seq_id[self.seq_no]
            self.photo_no = 0
            self.get_current_photos()
            self._refresh_img()

    def _next_seq(self, event=None):
        new_no = min(self.filtered_seqs.shape[0] - 1, self.seq_no + 1)
        if new_no > self.seq_no:
            self.seq_no = new_no
            self.current_seq = self.filtered_seqs.seq_id[self.seq_no]
            self.photo_no = 0
            self.get_current_photos()
            self._refresh_img()

    def _refresh_img(self):
        self.canvas.delete("all")
        if self.displayed_photo.path:
            self.img = Image.open(os.path.join(self.photo_dir, self.displayed_photo.path))
            self.orig_w, self.orig_h = self.img.size
            if self.w is None and self.h is None:
                self.w, self.h = self.orig_w, self.orig_h
            self.w_ratio = self.w / self.orig_w
            self.h_ratio = self.h / self.orig_h
            temp_img = self.img.resize((self.w, self.h))
            self.tk_img = ImageTk.PhotoImage(temp_img)
            self.canvas.create_image(0, 0, anchor="nw", image=self.tk_img)
        self.get_current_ratings()
        self._draw_ratings()
        self._reset_info()

    def _reset_info(self):
        self.seq_str.set(f"{self.current_seq}")
        self.seq_entry['width'] = len(self.seq_str.get())
        self.sno_str.set(f"{self.seq_no + 1}/{self.filtered_seqs.shape[0]}")
        self.sno_entry['width'] = len(self.sno_str.get())
        self.pno_str.set(f"{self.photo_no + 1}/{self.current_photos.shape[0]}")
        self.pno_entry['width'] = len(self.pno_str.get())
        if self.displayed_photo.path:
            self.os_path = os.path.normpath(os.path.join(self.photo_dir, self.displayed_photo.path))
        else:
            self.os_path = None
        self.path_str.set(f"{self.os_path}")
        self.path_entry['width'] = len(self.path_str.get())
        self.dbpath_str.set(f"{self.displayed_photo.path}")
        self.dbpath_entry['width'] = len(self.dbpath_str.get())
        self.hash_str.set(f"{self.displayed_photo.md5hash}")
        self.hash_entry['width'] = len(self.hash_str.get())

    def _draw_ratings(self):
        # look into vectorization
        for x in self.current_ratings.itertuples():
            color = next((y['color'] for y in COLORS if y['score'] == int(x.rating)), ['#000000'])
            bbox_orig = [x.bbox_x1, x.bbox_y1, x.bbox_x2, x.bbox_y2]
            bbox = [int(round(x, 0)) for x in [x.bbox_x1 * self.w_ratio, x.bbox_y1 * self.h_ratio,
                    x.bbox_x2 * self.w_ratio, x.bbox_y2 * self.h_ratio]]
            bbox_id = self.canvas.create_rectangle(bbox[0], bbox[1], bbox[2], bbox[3], width=2, outline=color,
                                                   tags=(x.scorer_name, x.rating))
            name_id = self.canvas.create_text(bbox[0], bbox[1], text=x.scorer_name,
                                              fill=color, anchor='sw',
                                              font=Font(size=max(int(round(14*self.h_ratio, 0)), 8),
                                                        weight='bold'))
            score_id = self.canvas.create_text(bbox[2], bbox[3], text=str(int(x.rating)),
                                               fill=color, anchor='ne',
                                               font=Font(size=max(int(round(14*self.h_ratio, 0)), 8),
                                                         weight='bold'))

    # entry callbacks
    # %d = Type of action (1=insert, 0=delete, -1 for others)
    # %i = index of char string to be inserted/deleted, or -1
    # %P = value of the entry if the edit is allowed
    # %s = value of entry prior to editing
    # %S = the text string being inserted or deleted, if any
    # %v = the type of validation that is currently set
    # %V = the type of validation that triggered the callback
    #      (key, focusin, focusout, forced)
    # %W = the tk name of the widget

    def _date_lower_change(self, d, i, s, S, P, V):
        print(V)
        return self._validate_date_entry(date_str=P)

    def _date_lower_focusin(self, event):
        print("focusin")
        self._unbind_arrows()
        if self.date_lower_str.get() == "yyyy-mm-dd":
            self.date_lower_str.set("")
            self.date_lower.config(fg="black")

    def _date_lower_focusout(self, event):
        print("focusout")
        self._bind_arrows()
        if self.date_lower_str.get() == "":
            self.date_lower_str.set("yyyy-mm-dd")
            self.date_lower.config(fg="gray50")
        if self._validate_date_entry(date_str=self.date_lower_str.get()):
            self._filter_seqs()

    def _date_higher_change(self, d, i, s, S, P, V):
        print(V)
        return self._validate_date_entry(date_str=P)

    def _date_higher_focusin(self, event):
        self._unbind_arrows()
        print("focusin")
        if self.date_higher_str.get() == "yyyy-mm-dd":
            self.date_higher_str.set("")
            self.date_higher.config(fg="black")

    def _date_higher_focusout(self, event):
        print("focusout")
        self._bind_arrows()
        if self.date_higher_str.get() == "":
            self.date_higher_str.set("yyyy-mm-dd")
            self.date_higher.config(fg="gray50")
        if self._validate_date_entry(date_str=self.date_higher_str.get()):
            self._filter_seqs()

    def _validate_date_entry(self, date_str):
        regex_list = [r'\d', r'\d', r'\d', r'\d', r'\-', r'\d', r'\d', r'\-', r'\d', r'\d']
        pattern = re.compile(''.join(('^', ''.join(regex_list[0:len(date_str)]), '$')))
        print(pattern.pattern)
        if re.match(pattern, date_str) is not None or date_str in ["yyyy-mm-dd", ""]:
            print('valid entry')
            return True
        else:
            print('invalid entry')
            self.bell()
            return False

    def _score_change(self,  d, i, s, S, P, V):
        if P != '' and P is not None:
            try:
                int(P)
                return True
            except ValueError:
                self.bell()
                return False
        else:
            return True

    @staticmethod
    def validate_date(date_str):
        parsed_date = None
        if re.match(r'^\d{4}\-\d{2}\-\d{2}$', date_str) is not None:
            try:
                parsed_date = parse(date_str, fuzzy=False)
            except ValueError:
                pass
        return parsed_date

    def _filter_seqs(self, force=False, event=None):
        query_list = []
        min_dt_allowed = self.validate_date(self.date_lower_str.get())
        max_dt_allowed = self.validate_date(self.date_higher_str.get())
        # sites = [x.replace("'", "") for x in self.site_name_str.get().removeprefix('(').removesuffix(')').split(', ')]
        selected_sites = [self.site_name.get(x) for x in self.site_name.curselection()]
        new_seq_filter = {'min_dt': min_dt_allowed, 'max_dt': max_dt_allowed, 'site_name': selected_sites}
        print(new_seq_filter, self.last_seq_filter)
        if new_seq_filter != self.last_seq_filter or force:
            print("seq_filter difference")
            if min_dt_allowed:
                # creates a pandas Series where each date string filter has had the timezone of the values its being
                # compared against replaced, thus we can compare the datetime in the df to a tz aware representation of
                # the filter condition.
                filter_dates_min = self.rated_seqs.min_dt.apply(lambda x: min_dt_allowed.replace(tzinfo=x.tzinfo))
                query_list.append("min_dt >= @filter_dates_min")
            if max_dt_allowed:
                filter_dates_max = self.rated_seqs.max_dt.apply(lambda x: max_dt_allowed.replace(tzinfo=x.tzinfo))
                query_list.append("max_dt <= @filter_dates_max")
            if selected_sites:
                query_list.append("site_name in @selected_sites")
            if query_list:
                query_str = ' and '.join(query_list)
                self.filtered_seqs = self.rated_seqs.query(query_str).sort_values(by=['min_dt']).reset_index()
            else:
                self.filtered_seqs = self.rated_seqs.copy()
                self.filtered_photos = self.photos.copy()

            self.last_seq_filter = new_seq_filter

        # restrict by rating filter
        if self.rating_seqs.shape[0] > 0:
            self.filtered_seqs = pd.merge(self.filtered_seqs, self.rating_seqs, on=['seq_id'], how='inner').\
                reset_index(drop=True).sort_values(by=['seq_id'])
            print(self.filtered_seqs)
        if self.rating_hashes.shape[0] > 0:
            self.filtered_photos = pd.merge(self.filtered_photos, self.rating_hashes, on=['md5hash'], how='inner').\
                reset_index(drop=True).sort_values(by=['seq_id', 'md5hash'])
            print(self.filtered_photos)

        if self.filtered_seqs.shape[0] > 0:
            self.seq_no = 0
            self.current_seq = self.filtered_seqs.seq_id[self.seq_no]
        else:
            self.seq_no = -1
            self.current_seq = None
        self.get_current_photos()
        self._refresh_img()

        # refresh options
        self.get_sites()

    def _filter_ratings(self, event=None):
        rating_query_list = []
        if self.score_lower_str.get():
            score_low = int(self.score_lower_str.get())
        else:
            score_low = None
        if self.score_higher_str.get():
            score_high = int(self.score_higher_str.get())
        else:
            score_high = None
        new_rating_filter = {'score_low': score_low, 'score_high': score_high}
        print(new_rating_filter, self.last_rating_filter)
        if new_rating_filter != self.last_rating_filter:
            print("rating_filter difference")
            if score_low:
                rating_query_list.append("rating >= @score_low")
            if score_high:
                rating_query_list.append("rating <= @score_high")
            if rating_query_list:
                rating_query_str = ' and '.join(rating_query_list)
                self.filtered_ratings = self.ratings.query(rating_query_str).reset_index()
                # print(self.filtered_ratings[['seq_id', 'md5hash']].sort_values(by=['seq_id', 'md5hash']))
                self.rating_hashes = self.filtered_ratings.groupby('md5hash', as_index=False)['md5hash'].last()
                self.rating_seqs = self.filtered_ratings.groupby('seq_id', as_index=False)['seq_id'].last()
            else:
                # reset
                self.filtered_ratings = self.ratings.copy()
                self.rating_hashes = pd.DataFrame()
                self.rating_seqs = pd.DataFrame()
                self.last_seq_filter = {'min_dt': None, 'max_dt': None, 'site_name': []}
            self.last_rating_filter = new_rating_filter

            # refresh options
            self._filter_seqs(force=True)


# if __name__ == "__main__":
viewer = PhotoViewer(dbpath=r'D:\animals\animal.sqlite', photo_dir=r'G:\GIS\Photos\trailcam')
viewer.mainloop()
