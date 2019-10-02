CREATE PROJECTION public.bikes_DBD_1_rep_ex /*+createtype(D)*/
    (
     bike_id ENCODING AUTO,
     owner_name ENCODING AUTO
        )
    AS
        SELECT bike_id,
               owner_name
        FROM public.bikes
        ORDER BY bike_id
    UNSEGMENTED ALL NODES;

CREATE PROJECTION public.test_DBD_2_rep_ex /*+createtype(D)*/
    (
     "time" ENCODING COMMONDELTA_COMP,
     bike_id ENCODING RLE,
     s_0 ENCODING AUTO,
     s_1 ENCODING AUTO,
     s_2 ENCODING AUTO,
     s_3 ENCODING AUTO,
     s_4 ENCODING AUTO,
     s_5 ENCODING AUTO,
     s_6 ENCODING RLE,
     s_7 ENCODING AUTO,
     s_8 ENCODING AUTO,
     s_9 ENCODING AUTO,
     s_10 ENCODING AUTO,
     s_11 ENCODING AUTO,
     s_12 ENCODING AUTO,
     s_13 ENCODING RLE,
     s_14 ENCODING RLE,
     s_15 ENCODING RLE,
     s_16 ENCODING RLE,
     s_17 ENCODING RLE,
     s_18 ENCODING RLE,
     s_19 ENCODING RLE,
     s_20 ENCODING RLE,
     s_21 ENCODING AUTO,
     s_22 ENCODING AUTO,
     s_23 ENCODING AUTO,
     s_24 ENCODING AUTO,
     s_25 ENCODING AUTO,
     s_26 ENCODING AUTO,
     s_27 ENCODING RLE,
     s_28 ENCODING RLE,
     s_29 ENCODING RLE,
     s_30 ENCODING RLE,
     s_31 ENCODING RLE,
     s_32 ENCODING AUTO,
     s_33 ENCODING RLE,
     s_34 ENCODING RLE,
     s_35 ENCODING RLE,
     s_36 ENCODING RLE,
     s_37 ENCODING RLE,
     s_38 ENCODING RLE,
     s_39 ENCODING RLE,
     s_40 ENCODING AUTO,
     s_41 ENCODING AUTO
        )
    AS
        SELECT "time",
               bike_id,
               s_0,
               s_1,
               s_2,
               s_3,
               s_4,
               s_5,
               s_6,
               s_7,
               s_8,
               s_9,
               s_10,
               s_11,
               s_12,
               s_13,
               s_14,
               s_15,
               s_16,
               s_17,
               s_18,
               s_19,
               s_20,
               s_21,
               s_22,
               s_23,
               s_24,
               s_25,
               s_26,
               s_27,
               s_28,
               s_29,
               s_30,
               s_31,
               s_32,
               s_33,
               s_34,
               s_35,
               s_36,
               s_37,
               s_38,
               s_39,
               s_40,
               s_41
        FROM public.test
        ORDER BY bike_id,
                 "time"
    UNSEGMENTED ALL NODES;