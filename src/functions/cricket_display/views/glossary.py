"""Glossary page — plain-English explanations of all ML and cricket analytics terms."""
from ._common import func


def view_glossary_html(req: func.HttpRequest) -> func.HttpResponse:
    html = """<!DOCTYPE html>
<html>
<head>
    <title>Glossary — Cricket Analytics Platform</title>
    <meta charset="utf-8">
    <style>
        body { font-family: Arial, sans-serif; background:#f5f5f5; margin:0; padding:30px; max-width:900px; }
        h1 { margin-bottom:6px; }
        h2 { margin-top:36px; margin-bottom:4px; border-bottom:2px solid #ddd; padding-bottom:6px; color:#222; }
        .subtitle { color:#666; margin-bottom:28px; font-size:15px; }
        .term { background:white; border-radius:8px; box-shadow:0 1px 4px #ccc;
                padding:18px 22px; margin-bottom:14px; }
        .term-name { font-size:16px; font-weight:bold; color:#0055aa; font-family:monospace; }
        .term-aka { font-size:13px; color:#888; margin-left:10px; font-family:sans-serif; font-style:italic; }
        .term-body { margin-top:8px; font-size:14px; color:#333; line-height:1.7; }
        .term-body strong { color:#111; }
        .term-body .example { background:#f0f4ff; border-left:3px solid #0055aa;
                               padding:8px 12px; border-radius:3px; margin-top:8px;
                               font-size:13px; color:#333; }
        .term-body .good { color:#2d7a2d; font-weight:bold; }
        .term-body .bad  { color:#cc2200; font-weight:bold; }
        nav { margin-bottom:24px; font-size:14px; }
        nav a { color:#0066cc; text-decoration:none; margin-right:16px; }
        nav a:hover { text-decoration:underline; }
        .toc { background:white; border-radius:8px; box-shadow:0 1px 4px #ccc;
               padding:16px 22px; margin-bottom:28px; }
        .toc-title { font-weight:bold; margin-bottom:8px; font-size:14px; color:#333; }
        .toc a { color:#0066cc; text-decoration:none; font-size:13px; margin-right:18px;
                 display:inline-block; margin-bottom:4px; }
        .toc a:hover { text-decoration:underline; }
    </style>
</head>
<body>
    <nav>
        <a href="/api/home">Home</a>
        <a href="/api/ml/win-predictor">Win Predictor</a>
        <a href="/api/ml/score-predictor">Score Predictor</a>
        <a href="/api/innings-tracker">Innings Tracker</a>
        <a href="/api/ml/glossary">Glossary</a>
    </nav>

    <h1>Glossary</h1>
    <p class="subtitle">Plain-English explanations of every metric, algorithm, and cricket term used across this platform.</p>

    <div class="toc">
        <div class="toc-title">Jump to section</div>
        <a href="#ml-metrics">ML Metrics</a>
        <a href="#algorithms">Algorithms</a>
        <a href="#model-concepts">Model Concepts</a>
        <a href="#cricket-terms">Cricket Terms</a>
        <a href="#odds-terms">Odds & Markets</a>
    </div>

    <!-- ══════════════════════════════════════════════ -->
    <h2 id="ml-metrics">ML Metrics</h2>

    <div class="term">
        <span class="term-name">Accuracy</span>
        <div class="term-body">
            The percentage of test matches where the model predicted the correct outcome.
            Used by the <strong>Win Predictor</strong> — the two outcomes are "chase won" and "defended".
            <br><br>
            <strong>Formula:</strong> correct predictions ÷ total predictions × 100
            <div class="example">
                Example: model tested on 8 matches, got 6 right → Accuracy = 75%
            </div>
            <br>
            <span class="good">≥ 70% = good</span> &nbsp;
            <span style="color:#cc7700;font-weight:bold">60–70% = acceptable</span> &nbsp;
            <span class="bad">&lt; 60% = near random</span>
            <br><br>
            <strong>Caveat:</strong> with fewer than 10 test matches, one wrong prediction changes accuracy by 10–15
            percentage points. Treat it as a guide, not a precise score.
        </div>
    </div>

    <div class="term">
        <span class="term-name">ROC-AUC</span>
        <span class="term-aka">Area Under the ROC Curve</span>
        <div class="term-body">
            Measures how well the model separates the two outcomes using its <em>confidence score</em>,
            not just the final prediction. This makes it more informative than accuracy on small datasets.
            <br><br>
            <strong>Scale:</strong> 0.5 = random guessing (coin flip) &nbsp;|&nbsp; 1.0 = perfect separation
            <div class="example">
                Example: ROC-AUC of 0.82 means that if you pick one "chase won" match and one "defended" match at
                random, the model gives the correct match a higher confidence score 82% of the time.
            </div>
            <br>
            <span class="good">≥ 0.75 = strong</span> &nbsp;
            <span style="color:#cc7700;font-weight:bold">0.60–0.75 = acceptable</span> &nbsp;
            <span class="bad">&lt; 0.60 = weak</span>
        </div>
    </div>

    <div class="term">
        <span class="term-name">MAE</span>
        <span class="term-aka">Mean Absolute Error</span>
        <div class="term-body">
            The average number of runs the prediction was off by. Used by the <strong>Score Predictor</strong>.
            <br><br>
            <strong>Formula:</strong> average of |predicted score − actual score| across all test matches
            <div class="example">
                Example: MAE of 12 means on average the model predicted within ±12 runs of the actual final score.
                If the actual score was 172, most predictions fall between 160 and 184.
            </div>
            <br>
            <span class="good">≤ 10 runs = good</span> &nbsp;
            <span style="color:#cc7700;font-weight:bold">11–18 runs = acceptable</span> &nbsp;
            <span class="bad">&gt; 18 runs = needs more data</span>
            <br><br>
            With 30 matches, 12–18 run MAE is realistic. Improves as the dataset grows.
        </div>
    </div>

    <div class="term">
        <span class="term-name">RMSE</span>
        <span class="term-aka">Root Mean Squared Error</span>
        <div class="term-body">
            Like MAE, but large errors are penalised much more heavily. Used by the <strong>Score Predictor</strong>.
            <br><br>
            <strong>Formula:</strong> square root of the average of (predicted − actual)² across all test matches
            <div class="example">
                Example: three predictions off by 10 runs each (RMSE contribution: small) vs one prediction off by
                30 runs (RMSE contribution: large). The 30-run miss hurts the RMSE far more.
            </div>
            <br>
            RMSE is always <strong>≥ MAE</strong>. A large gap between them means the model has occasional blowout
            predictions — it's mostly accurate but sometimes wildly wrong.
            <br><br>
            Lower is better. Same colour thresholds as MAE.
        </div>
    </div>

    <div class="term">
        <span class="term-name">R²</span>
        <span class="term-aka">R-squared / Coefficient of Determination</span>
        <div class="term-body">
            How much of the variation in final scores the model explains. Used by the <strong>Score Predictor</strong>.
            <br><br>
            <strong>Scale:</strong>
            <ul style="margin:6px 0 6px 18px;line-height:1.8">
                <li><strong>1.0</strong> = perfect — model predicts every score exactly</li>
                <li><strong>0.0</strong> = no better than always predicting the average score</li>
                <li><strong>Negative</strong> = worse than just guessing the average every time</li>
            </ul>
            <div class="example">
                Example: R² of 0.55 means the model explains 55% of why scores differ match to match.
                The other 45% is noise, randomness, or factors the model doesn't see.
            </div>
            <br>
            <span class="good">≥ 0.60 = strong</span> &nbsp;
            <span style="color:#cc7700;font-weight:bold">0.30–0.60 = acceptable</span> &nbsp;
            <span class="bad">&lt; 0.30 = weak</span>
            <br><br>
            With 30 matches, 0.3–0.6 is realistic. Improves as the dataset grows.
        </div>
    </div>

    <div class="term">
        <span class="term-name">Confidence %</span>
        <div class="term-body">
            How certain the model is in its own prediction. Shown in the Win Predictor test match table.
            <br><br>
            This is the model's probability output converted to a percentage — not a percentage of the time it's right.
            <div class="example">
                Example: confidence 84% means the model's internal probability for its predicted outcome is 0.84.
                A correct prediction with 60% confidence is less convincing than one with 90% confidence.
            </div>
            A high-confidence wrong prediction is a useful signal that the model has a blind spot for that type of match.
        </div>
    </div>

    <!-- ══════════════════════════════════════════════ -->
    <h2 id="algorithms">Algorithms</h2>

    <div class="term">
        <span class="term-name">XGBoost</span>
        <span class="term-aka">XGB — eXtreme Gradient Boosting</span>
        <div class="term-body">
            The primary model on this platform. Builds a sequence of decision trees where each new tree
            corrects the mistakes of the previous ones (this is called "boosting").
            <br><br>
            <strong>Why it's used:</strong> handles correlated features well, works on small datasets, and produces
            feature importances that show which inputs actually drove each prediction.
            <br><br>
            <strong>Classifier</strong> (Win Predictor): predicts a category — "chase won" or "defended".<br>
            <strong>Regressor</strong> (Score Predictor): predicts a number — the final innings-1 score.
        </div>
    </div>

    <div class="term">
        <span class="term-name">Random Forest</span>
        <span class="term-aka">RF</span>
        <div class="term-body">
            Builds many independent decision trees in parallel (a "forest") and averages their predictions.
            Unlike XGBoost, each tree sees only a random subset of the data and features.
            <br><br>
            <strong>Why it's here:</strong> Random Forest is more stable on small datasets and its feature importances
            are less noisy than XGBoost's. It's trained as a cross-check — if both RF and XGB agree that a feature
            matters, the signal is genuine. If they disagree strongly, the dataset is too small to be confident.
            <br><br>
            XGBoost is the primary model; Random Forest is the sanity check.
        </div>
    </div>

    <!-- ══════════════════════════════════════════════ -->
    <h2 id="model-concepts">Model Concepts</h2>

    <div class="term">
        <span class="term-name">Train / Test Split</span>
        <div class="term-body">
            The dataset is split into two groups by date:
            <ul style="margin:6px 0 6px 18px;line-height:1.8">
                <li><strong>Training set</strong> — matches before the cutoff date. The model learns from these.</li>
                <li><strong>Test set</strong> — matches after the cutoff date. The model has never seen these.
                    Metrics (accuracy, MAE etc.) are measured only on the test set.</li>
            </ul>
            <div class="example">
                Why this matters: a model that memorises training data looks perfect on training matches but fails
                on new ones. Testing on unseen matches is the only honest measure of real-world performance.
            </div>
            The cutoff is set to today − 3 days for the Win Predictor and today − 5 days for the Score Predictor.
        </div>
    </div>

    <div class="term">
        <span class="term-name">Feature Importance</span>
        <div class="term-body">
            A ranking of which input variables (features) the model relied on most when making predictions.
            Shown as a percentage of total importance — higher means the model used that feature more heavily.
            <br><br>
            Features with zero importance are automatically dropped before the model is retrained (pruning).
            <div class="example">
                Example: if <code>inn2_ov6_chase_difficulty</code> has 18% importance, the model used it in 18%
                of its decision weight. If <code>venue</code> has 0.2%, it barely influenced anything.
            </div>
            Colour coding: <span style="color:#0055aa">■ blue = odds/market features</span>
            &nbsp; <span style="color:#6600aa">■ purple = composite features</span>
            &nbsp; <span style="color:#007755">■ green = team/venue categoricals</span>
        </div>
    </div>

    <div class="term">
        <span class="term-name">Feature Pruning</span>
        <div class="term-body">
            After the first training pass, any feature that accounts for less than 0.5% of total importance
            is dropped and the model is retrained on the reduced set. This prevents noise from irrelevant
            columns from diluting the signal and reduces overfitting on small datasets.
        </div>
    </div>

    <div class="term">
        <span class="term-name">Chase Difficulty</span>
        <span class="term-aka">inn2_ovN_chase_difficulty</span>
        <div class="term-body">
            A composite feature: <strong>RRR × (wickets fallen + 1)</strong>.
            <br><br>
            Amplifies the signal when both the required run rate is high <em>and</em> wickets are down.
            The five individual features — RRR, CRR, rr_diff, runs_needed, runs_needed_per_wkt — all encode
            similar information and dilute each other's importance when used separately.
            <div class="example">
                Example: 51/5 chasing 254 at over 6 → RRR ≈ 14.5, wickets fallen = 5 →
                chase_difficulty = 14.5 × 6 = <strong>87</strong> (near-impossible).<br>
                80/1 chasing 160 at over 6 → RRR ≈ 5.7, wickets fallen = 1 →
                chase_difficulty = 5.7 × 2 = <strong>11.4</strong> (comfortable).
            </div>
        </div>
    </div>

    <!-- ══════════════════════════════════════════════ -->
    <h2 id="cricket-terms">Cricket Terms Used in Features</h2>

    <div class="term">
        <span class="term-name">CRR</span>
        <span class="term-aka">Current Run Rate</span>
        <div class="term-body">
            Runs scored so far ÷ overs faced. Measures how fast the batting team has been scoring.
            <div class="example">Example: 80 runs in 8 overs → CRR = 10.0 runs per over</div>
        </div>
    </div>

    <div class="term">
        <span class="term-name">RRR</span>
        <span class="term-aka">Required Run Rate</span>
        <div class="term-body">
            Runs still needed ÷ overs remaining. Measures how fast the chasing team must score to win.
            <div class="example">Example: need 120 runs in 10 overs → RRR = 12.0 runs per over</div>
            In T20, above 12 is very difficult; above 15 is near-impossible.
        </div>
    </div>

    <div class="term">
        <span class="term-name">rr_diff</span>
        <span class="term-aka">Run Rate Differential</span>
        <div class="term-body">
            CRR − RRR. Positive = chasing team is ahead of the required rate (good for them).
            Negative = behind the required rate (need to accelerate).
            <div class="example">Example: CRR 9.0, RRR 8.0 → rr_diff = +1.0 → chase is on track</div>
        </div>
    </div>

    <div class="term">
        <span class="term-name">rp_wkt</span>
        <span class="term-aka">Runs Per Wicket In Hand</span>
        <div class="term-body">
            Score ÷ (10 − wickets fallen). Captures the joint effect of runs scored and wickets remaining.
            A team at 80/0 (8.0 per wicket) is in a different position to 80/4 (16.0 per wicket)
            even though the score is the same.
        </div>
    </div>

    <div class="term">
        <span class="term-name">Powerplay</span>
        <div class="term-body">
            Overs 1–6. Fielding restrictions apply — only 2 fielders allowed outside the 30-yard circle.
            Generally produces higher scoring and is the most volatile phase of T20 cricket.
            Models trained at the end of the powerplay (at-over-6 / innings2-6over) have less information
            than later models but are still useful for live match predictions.
        </div>
    </div>

    <div class="term">
        <span class="term-name">Death Overs</span>
        <div class="term-body">
            Overs 17–20. The final phase where batting teams typically accelerate and scoring is highest.
            Features like <code>inn1_death_runs</code> and <code>inn1_death_wickets</code> capture how
            well a team batted (or a bowling team contained them) in this phase.
        </div>
    </div>

    <div class="term">
        <span class="term-name">Bat Dominance</span>
        <span class="term-aka">inn1_bat_dominance</span>
        <div class="term-body">
            Maximum individual strike rate minus average strike rate across all innings-1 batsmen (min 5 balls).
            A high value means one batter dominated while others struggled — an innings carried by an individual.
            A low value means the whole team contributed evenly.
            <div class="example">
                Example: one batter scored at SR 180, others averaged SR 110 → bat_dominance = 70.
                This helps the model distinguish "one batter got lucky" from "team was genuinely strong".
            </div>
        </div>
    </div>

    <!-- ══════════════════════════════════════════════ -->
    <h2 id="odds-terms">Odds & Market Terms</h2>

    <div class="term">
        <span class="term-name">Batting Team Odds / Bowling Team Odds</span>
        <span class="term-aka">bat_odds / bowl_odds</span>
        <div class="term-body">
            Live decimal odds offered by Bet365 at each over boundary during the match.
            <br><br>
            <strong>Decimal odds:</strong> if bat_odds = 1.40, a ₹100 bet returns ₹140 (₹40 profit).
            Lower odds = that team is the favourite.
            <div class="example">
                Example: bat_odds = 1.25 → batting team is a strong favourite (80% implied probability).<br>
                bat_odds = 3.50 → batting team is unlikely to win (29% implied probability).
            </div>
            These are the most powerful features in the model — the betting market aggregates everything
            it knows (pitch conditions, team form, weather, matchups) into a single number.
        </div>
    </div>

    <div class="term">
        <span class="term-name">Implied Probability</span>
        <div class="term-body">
            The win probability the market is pricing in: <strong>1 ÷ odds</strong>.
            <div class="example">
                Example: bat_odds = 2.00 → implied probability = 50% (evenly matched).<br>
                bat_odds = 1.20 → implied probability = 83% (heavy favourite).
            </div>
            Note: bookmaker odds include a margin, so implied probabilities across all outcomes sum to
            slightly more than 100%.
        </div>
    </div>

    <div class="term">
        <span class="term-name">Odds Swing</span>
        <span class="term-aka">inn1_odds_swing_full / inn2_ovN_odds_swing</span>
        <div class="term-body">
            How far the batting team's odds moved between two points in the match.
            Positive = market moved toward the batting team (they performed well in that period).
            Negative = market moved against them (struggled).
            <div class="example">
                Example: odds went from 2.0 at over 1 to 1.4 at over 6 → odds_swing = −0.6
                (market moved toward batting team — they had a strong powerplay).
            </div>
        </div>
    </div>

    <div class="term">
        <span class="term-name">Over/Under Line</span>
        <span class="term-aka">predicted_total / over_odds / under_odds</span>
        <div class="term-body">
            A market offered by Bet365 on the total runs scored in innings 1.
            The <em>line</em> is the predicted total. Bettors can back "over" (final score exceeds the line)
            or "under" (final score is below it).
            <br><br>
            Used by the <strong>Innings Tracker</strong> to measure prediction accuracy by over stage,
            team, venue, and odds level.
        </div>
    </div>

</body>
</html>"""
    return func.HttpResponse(html, mimetype="text/html")
