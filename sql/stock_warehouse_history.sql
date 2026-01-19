-- ============================================================================
-- 在庫の倉庫履歴取得クエリ（基幹システム用）
-- ============================================================================
-- 概要:
--   指定時点での在庫の所在倉庫を取得するクエリ
--   開発担当者より提供されたクエリを貼り付け用
--
-- 用途:
--   償却資産税申告用に以下の情報を取得
--   - 期首時点（2025年1月1日）の所在倉庫
--   - 期末時点（2026年1月1日）の所在倉庫
--
-- 作成日: 2026-01-19
-- ============================================================================

WITH 
-- 1. 【復元処理】2026/1/1 0:00 時点の状態を確定
StockSnapshot AS (
    SELECT 
        s.id AS stock_id,
        
        -- ▼ 【修正】履歴がある場合は、NULLを含めて履歴の値を優先する（CASE文に変更）
        CASE WHEN h.id IS NOT NULL THEN h.status ELSE s.status END AS status,
        
        -- ▼ 【追加】deleted_at の復元
        -- 履歴があれば履歴の値(h)を使用。
        -- 履歴がなく現在データ(s)を使う場合も、未来の日付(1/1以降の削除)なら NULL(未削除) 扱いにする。
        CASE 
            WHEN h.id IS NOT NULL THEN h.stock_deleted_at 
            WHEN s.deleted_at <= '2026-01-01 00:00:00' THEN s.deleted_at
            ELSE NULL 
        END AS restored_deleted_at,

        -- ▼ impossibled_at も同様に厳密化
        CASE 
            WHEN h.id IS NOT NULL THEN h.impossibled_at 
            WHEN s.impossibled_at <= '2026-01-01 00:00:00' THEN s.impossibled_at
            ELSE NULL 
        END AS restored_impossibled_at,

        -- location_id
        CASE WHEN h.id IS NOT NULL THEN h.location_id ELSE s.location_id END AS restored_location_id,

        s.created_at as stock_created_at
    FROM 
        stock s
    LEFT JOIN (
        SELECT id, stock_id, location_id, status, impossibled_at, stock_deleted_at
        FROM (
            SELECT 
                id, stock_id, location_id, status, impossibled_at, stock_deleted_at,
                ROW_NUMBER() OVER (PARTITION BY stock_id ORDER BY created_at DESC, id DESC) as rn
            FROM stock_history
            WHERE created_at <= '2026-01-01 00:00:00'
        ) sub 
        WHERE rn = 1
    ) h ON s.id = h.stock_id
    WHERE 
        s.created_at <= '2026-01-01 00:00:00'
),

-- 2. 【追跡処理】過去の倉庫履歴を取得（変更なし）
LastWarehouseHistory AS (
    SELECT 
        stock_id,
        location_id
    FROM (
        SELECT 
            h.stock_id,
            h.location_id,
            ROW_NUMBER() OVER (PARTITION BY h.stock_id ORDER BY h.created_at DESC) as rn
        FROM 
            stock_history h
        INNER JOIN 
            location l ON h.location_id = l.id 
        WHERE 
            h.created_at <= '2026-01-01 00:00:00'
            AND l.warehouse_id IS NOT NULL 
    ) sub
    WHERE rn = 1 
)

-- 3. 【出力】
SELECT 
    ss.stock_id,
    
    -- ▼ 復元されたステータスと日付
    ss.status AS status_at_jan1,
    ss.restored_deleted_at AS deleted_at_jan1,       -- ★ここに追加されました
    ss.restored_impossibled_at AS impossibled_at_jan1,

    -- ▼ 所在倉庫ID (1.現在地 -> 2.履歴 -> 3.船橋)
    COALESCE(l_curr.warehouse_id, l_last.warehouse_id, 21) AS final_warehouse_id,

    -- ▼ 所在倉庫名（リスト対応）
    CASE COALESCE(l_curr.warehouse_id, l_last.warehouse_id, 21)
        WHEN 1 THEN '青葉台オフィス'
        WHEN 2 THEN '株式会社樋口物流サービス'
        WHEN 3 THEN '（未使用）大阪'
        WHEN 4 THEN 'ケイシン倉庫'
        WHEN 5 THEN '仙台'
        WHEN 6 THEN '福岡'
        WHEN 7 THEN '名古屋'
        WHEN 8 THEN '外部倉庫'
        WHEN 9 THEN 'ロジボン（関東）'
        WHEN 10 THEN '三鷹'
        WHEN 11 THEN 'バーチ倉庫-A'
        WHEN 12 THEN '日通倉庫'
        WHEN 13 THEN 'シンメイ茨木'
        WHEN 14 THEN '有限会社ポルテ'
        WHEN 15 THEN '（未使用）バーチ倉庫-B'
        WHEN 16 THEN 'バーチ倉庫-C'
        WHEN 17 THEN '大西運輸'
        WHEN 18 THEN '戸田倉庫'
        WHEN 19 THEN '大翔トランスポート株式会社'
        WHEN 20 THEN '法人直送'
        WHEN 21 THEN '船橋'
        WHEN 22 THEN '市川サテライト倉庫'
        WHEN 23 THEN '東葛西ロジスティクスセンター'
        WHEN 24 THEN '伊丹創治郎倉庫'
        WHEN 25 THEN 'キタザワ関西倉庫'
        WHEN 26 THEN 'EC直送'
        WHEN 27 THEN '株式会社MAKE VALUE　川崎倉庫'
        WHEN 28 THEN 'エイトレント西宮倉庫'
        WHEN 29 THEN 'キタザワ九州倉庫'
        WHEN 30 THEN '門真倉庫'
        ELSE 'その他・不明'
    END AS final_warehouse_name

FROM 
    StockSnapshot ss
    LEFT JOIN location l_curr ON ss.restored_location_id = l_curr.id
    LEFT JOIN LastWarehouseHistory lwh ON ss.stock_id = lwh.stock_id
    LEFT JOIN location l_last ON lwh.location_id = l_last.id
;


