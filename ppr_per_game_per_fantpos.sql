CREATE OR REPLACE VIEW public.ppr_per_game_per_fantpos AS
 WITH ppr_by_game AS (
         SELECT g.season,
            g.gsis_id,
            gt.player_id,
            gt.full_name,
            gt."position",
            gt.team,
            gt.pass_yards / 25 + gt.pass_tds * 4 - gt.pass_ints * 2 + gt.recs + gt.rec_tds * 6 + gt.rec_yards / 10 + gt.rush_yards / 10 + gt.rush_tds * 6 - gt.fumbles * 2 AS ppr
           FROM ( SELECT b.gsis_id,
                    a.player_id,
                    a.full_name,
                    c."position",
                    b.team,
                    sum(b.passing_yds) AS pass_yards,
                    sum(b.passing_int) AS pass_ints,
                    sum(b.passing_tds) AS pass_tds,
                    sum(b.receiving_rec) AS recs,
                    sum(b.receiving_tds) AS rec_tds,
                    sum(b.receiving_yds) AS rec_yards,
                    sum(b.rushing_tds) AS rush_tds,
                    sum(b.rushing_yds) AS rush_yards,
                    sum(b.fumbles_lost) AS fumbles
                   FROM player a
                     LEFT JOIN play_player b ON a.player_id::text = b.player_id::text
                     LEFT JOIN ( SELECT c4.player_id,
                            c4.full_name,
                                CASE
                                    WHEN c4."position"::text = 'WR_TE'::text AND c4.weight::smallint > 249 THEN 'TE'::character varying
                                    WHEN c4."position"::text = 'WR_TE'::text AND c4.weight::smallint < 250 THEN 'WR'::character varying
                                    ELSE c4."position"
                                END AS "position"
                           FROM ( SELECT c1.player_id,
                                    c1.full_name,
                                    c1.weight,
CASE
 WHEN c1."position" = 'UNK'::player_pos THEN c3.pos::character varying
 ELSE c1."position"::character varying
END AS "position"
                                   FROM player c1
                                     LEFT JOIN ( SELECT c2.player_id,
  CASE
   WHEN c2.pass_yards > c2.rec_yards AND c2.pass_yards > c2.rush_yards THEN 'QB'::text
   WHEN c2.rec_yards > c2.pass_yards AND c2.rec_yards > c2.rush_yards THEN 'WR_TE'::text
   WHEN c2.rush_yards > c2.pass_yards AND c2.rush_yards > c2.rec_yards THEN 'RB'::text
   ELSE 'UNK'::text
  END AS pos
   FROM ( SELECT play_player.player_id,
      sum(play_player.passing_yds) AS pass_yards,
      sum(play_player.receiving_yds) AS rec_yards,
      sum(play_player.rushing_yds) AS rush_yards
     FROM play_player
    GROUP BY play_player.player_id) c2) c3 ON c1.player_id::text = c3.player_id::text
                                  WHERE c1."position" = 'UNK'::player_pos AND c3.pos <> 'UNK'::text) c4) c ON a.player_id::text = c.player_id::text
                  WHERE a."position" = ANY (ARRAY['QB'::player_pos, 'WR'::player_pos, 'TE'::player_pos, 'RB'::player_pos, 'UNK'::player_pos])
                  GROUP BY b.gsis_id, a.player_id, a.full_name, c."position", b.team) gt
             JOIN ( SELECT game.gsis_id,
                    game.season_year AS season
                   FROM game
                  WHERE game.season_type = 'Regular'::season_phase) g ON gt.gsis_id::text = g.gsis_id::text
        ), pos_rank AS (
         SELECT a.season,
            a.gsis_id,
            a.full_name,
            a."position",
            a.team,
            a.ppr,
            b.pos
           FROM ppr_by_game a
             JOIN ( SELECT b1.season,
                    b1.player_id,
                    b1."position"::text || row_number() OVER (PARTITION BY b1.season, b1."position", b1.team ORDER BY b1.ppr DESC)::character varying::text AS pos
                   FROM ( SELECT ppr_by_game.season,
                            ppr_by_game.player_id,
                            ppr_by_game."position",
                            ppr_by_game.team,
                            sum(ppr_by_game.ppr) AS ppr
                           FROM ppr_by_game
                          GROUP BY ppr_by_game.season, ppr_by_game.player_id, ppr_by_game."position", ppr_by_game.team) b1) b ON a.season::smallint = b.season::smallint AND a.player_id::text = b.player_id::text
        )
 SELECT f.qb_ppr,
    COALESCE(f.wr1_ppr, f.wr2_ppr, f.wr3_ppr, f.wr4_ppr, f.wr5_ppr, 0::bigint) AS wr1_ppr,
        CASE
            WHEN f.wr1_ppr IS NOT NULL THEN COALESCE(f.wr2_ppr, f.wr3_ppr, f.wr4_ppr, f.wr5_ppr, 0::bigint)
            ELSE COALESCE(f.wr3_ppr, f.wr4_ppr, f.wr5_ppr, 0::bigint)
        END AS wr2_ppr,
        CASE
            WHEN f.wr1_ppr IS NOT NULL AND f.wr2_ppr IS NOT NULL THEN COALESCE(f.wr3_ppr, f.wr4_ppr, f.wr5_ppr, 0::bigint)
            ELSE COALESCE(f.wr4_ppr, f.wr5_ppr, 0::bigint)
        END AS wr3_ppr,
    COALESCE(f.rb1_ppr, f.rb2_ppr, f.rb3_ppr, f.rb4_ppr, 0::bigint) AS rb1_ppr,
        CASE
            WHEN f.rb1_ppr IS NOT NULL THEN COALESCE(f.rb2_ppr, f.rb3_ppr, f.rb4_ppr, 0::bigint)
            ELSE COALESCE(f.rb3_ppr, f.rb4_ppr, 0::bigint)
        END AS rb2_ppr,
    COALESCE(f.te1_ppr, f.te2_ppr, f.te3_ppr, 0::bigint) AS te1_ppr,
        CASE
            WHEN f.te1_ppr IS NOT NULL THEN COALESCE(f.te2_ppr, f.te3_ppr, 0::bigint)
            ELSE COALESCE(f.te3_ppr, 0::bigint)
        END AS te2_ppr
   FROM ( SELECT qb.season,
            qb.team,
            qb.full_name AS qb,
            qb.ppr AS qb_ppr,
            wr1.full_name AS wr1,
            wr1.ppr AS wr1_ppr,
            wr2.full_name AS wr2,
            wr2.ppr AS wr2_ppr,
            wr3.full_name AS wr3,
            wr3.ppr AS wr3_ppr,
            wr4.full_name AS wr4,
            wr4.ppr AS wr4_ppr,
            wr5.full_name AS wr5,
            wr5.ppr AS wr5_ppr,
            rb1.full_name AS rb1,
            rb1.ppr AS rb1_ppr,
            rb2.full_name AS rb2,
            rb2.ppr AS rb2_ppr,
            rb3.full_name AS rb3,
            rb3.ppr AS rb3_ppr,
            rb4.full_name AS rb4,
            rb4.ppr AS rb4_ppr,
            te1.full_name AS te1,
            te1.ppr AS te1_ppr,
            te2.full_name AS te2,
            te2.ppr AS te2_ppr,
            te3.full_name AS te3,
            te3.ppr AS te3_ppr
           FROM ( SELECT pos_rank.season,
                    pos_rank.gsis_id,
                    pos_rank.full_name,
                    pos_rank."position",
                    pos_rank.team,
                    pos_rank.ppr,
                    pos_rank.pos
                   FROM pos_rank
                  WHERE pos_rank.pos = 'QB1'::text) qb
             LEFT JOIN ( SELECT pos_rank.season,
                    pos_rank.gsis_id,
                    pos_rank.full_name,
                    pos_rank."position",
                    pos_rank.team,
                    pos_rank.ppr,
                    pos_rank.pos
                   FROM pos_rank
                  WHERE pos_rank.pos = 'WR1'::text) wr1 ON qb.gsis_id::text = wr1.gsis_id::text AND qb.team::text = wr1.team::text
             LEFT JOIN ( SELECT pos_rank.season,
                    pos_rank.gsis_id,
                    pos_rank.full_name,
                    pos_rank."position",
                    pos_rank.team,
                    pos_rank.ppr,
                    pos_rank.pos
                   FROM pos_rank
                  WHERE pos_rank.pos = 'WR2'::text) wr2 ON qb.gsis_id::text = wr2.gsis_id::text AND qb.team::text = wr2.team::text
             LEFT JOIN ( SELECT pos_rank.season,
                    pos_rank.gsis_id,
                    pos_rank.full_name,
                    pos_rank."position",
                    pos_rank.team,
                    pos_rank.ppr,
                    pos_rank.pos
                   FROM pos_rank
                  WHERE pos_rank.pos = 'WR3'::text) wr3 ON qb.gsis_id::text = wr3.gsis_id::text AND qb.team::text = wr3.team::text
             LEFT JOIN ( SELECT pos_rank.season,
                    pos_rank.gsis_id,
                    pos_rank.full_name,
                    pos_rank."position",
                    pos_rank.team,
                    pos_rank.ppr,
                    pos_rank.pos
                   FROM pos_rank
                  WHERE pos_rank.pos = 'WR4'::text) wr4 ON qb.gsis_id::text = wr4.gsis_id::text AND qb.team::text = wr4.team::text
             LEFT JOIN ( SELECT pos_rank.season,
                    pos_rank.gsis_id,
                    pos_rank.full_name,
                    pos_rank."position",
                    pos_rank.team,
                    pos_rank.ppr,
                    pos_rank.pos
                   FROM pos_rank
                  WHERE pos_rank.pos = 'WR5'::text) wr5 ON qb.gsis_id::text = wr5.gsis_id::text AND qb.team::text = wr5.team::text
             LEFT JOIN ( SELECT pos_rank.season,
                    pos_rank.gsis_id,
                    pos_rank.full_name,
                    pos_rank."position",
                    pos_rank.team,
                    pos_rank.ppr,
                    pos_rank.pos
                   FROM pos_rank
                  WHERE pos_rank.pos = 'RB1'::text) rb1 ON qb.gsis_id::text = rb1.gsis_id::text AND qb.team::text = rb1.team::text
             LEFT JOIN ( SELECT pos_rank.season,
                    pos_rank.gsis_id,
                    pos_rank.full_name,
                    pos_rank."position",
                    pos_rank.team,
                    pos_rank.ppr,
                    pos_rank.pos
                   FROM pos_rank
                  WHERE pos_rank.pos = 'RB2'::text) rb2 ON qb.gsis_id::text = rb2.gsis_id::text AND qb.team::text = rb2.team::text
             LEFT JOIN ( SELECT pos_rank.season,
                    pos_rank.gsis_id,
                    pos_rank.full_name,
                    pos_rank."position",
                    pos_rank.team,
                    pos_rank.ppr,
                    pos_rank.pos
                   FROM pos_rank
                  WHERE pos_rank.pos = 'RB3'::text) rb3 ON qb.gsis_id::text = rb3.gsis_id::text AND qb.team::text = rb3.team::text
             LEFT JOIN ( SELECT pos_rank.season,
                    pos_rank.gsis_id,
                    pos_rank.full_name,
                    pos_rank."position",
                    pos_rank.team,
                    pos_rank.ppr,
                    pos_rank.pos
                   FROM pos_rank
                  WHERE pos_rank.pos = 'RB4'::text) rb4 ON qb.gsis_id::text = rb4.gsis_id::text AND qb.team::text = rb4.team::text
             LEFT JOIN ( SELECT pos_rank.season,
                    pos_rank.gsis_id,
                    pos_rank.full_name,
                    pos_rank."position",
                    pos_rank.team,
                    pos_rank.ppr,
                    pos_rank.pos
                   FROM pos_rank
                  WHERE pos_rank.pos = 'TE1'::text) te1 ON qb.gsis_id::text = te1.gsis_id::text AND qb.team::text = te1.team::text
             LEFT JOIN ( SELECT pos_rank.season,
                    pos_rank.gsis_id,
                    pos_rank.full_name,
                    pos_rank."position",
                    pos_rank.team,
                    pos_rank.ppr,
                    pos_rank.pos
                   FROM pos_rank
                  WHERE pos_rank.pos = 'TE2'::text) te2 ON qb.gsis_id::text = te2.gsis_id::text AND qb.team::text = te2.team::text
             LEFT JOIN ( SELECT pos_rank.season,
                    pos_rank.gsis_id,
                    pos_rank.full_name,
                    pos_rank."position",
                    pos_rank.team,
                    pos_rank.ppr,
                    pos_rank.pos
                   FROM pos_rank
                  WHERE pos_rank.pos = 'TE3'::text) te3 ON qb.gsis_id::text = te3.gsis_id::text AND qb.team::text = te3.team::text) f;