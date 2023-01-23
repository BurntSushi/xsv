#[cfg(feature = "lang")]
mod test {
    use workdir::Workdir;

    #[test]
    fn lang() {
        let wrk = Workdir::new("lang");
        wrk.create("data.csv", vec![
            svec!["text"],
            svec!["Hello world"],
            svec!["Une phrase en français."],
            svec!["Familie Müller plant ihren Urlaub.
                Sie geht in ein Reisebüro und lässt sich von einem Angestellten beraten.
                Als Reiseziel wählt sie Mallorca aus.
                Familie Müller bucht einen Flug auf die Mittelmeerinsel.
                Sie bucht außerdem zwei Zimmer in einem großen Hotel direkt am Strand.
                Familie Müller badet gerne im Meer.
                Am Abflugtag fahren Herr und Frau Müller mit ihren beiden Kindern im Taxi zum Flughafen.
                Dort warten schon viele Urlauber. Alle wollen nach Mallorca fliegen.
                Familie Müller hat viel Gepäck dabei: drei große Koffer und zwei Taschen.
                Die Taschen sind Handgepäck. Familie Müller nimmt sie mit in das Flugzeug.
                Am Flugschalter checkt die Familie ein und erhält ihre Bordkarten.
                Die Angestellte am Flugschalter erklärt Herrn Müller den Weg zum Flugsteig.
                Es ist nicht mehr viel Zeit bis zum Abflug.
                Familie Müller geht durch die Sicherheitskontrolle.
                Als alle das richtige Gate erreichen, setzen sie sich in den Wartebereich.
                Kurz darauf wird ihre Flugnummer aufgerufen und Familie Müller steigt mit vielen anderen Passagieren in das Flugzeug nach Mallorca.
                Beim Starten fühlt sich Herr Müller nicht wohl.
                Ihm wird ein wenig übel.
                Nach zwei Stunden landet das Flugzeug.
                Am Gepäckband warten alle Passagiere noch auf ihr fehlendes Gepäck.
                Danach kann endlich der Urlaub beginnen."],
            svec!["У меня большая семья из шести человек: я, мама, папа, старшая сестра, бабушка и дедушка.
                Мы живем все вместе с собакой Бимом и кошкой Муркой в большом доме в деревне.
                Мой папа встает раньше всех, потому что ему рано на работу. Он работает доктором.
                Обычно бабушка готовит нам завтрак.
                Я обожаю овсяную кашу, а моя сестра Аня – блины."],
        ]);
        let mut cmd = wrk.command("lang");
        cmd.arg("text").arg("data.csv");

        let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
        let expected = vec![
            svec!["text",
                "lang"],
            svec!["Hello world",
                "english"],
            svec!["Une phrase en français.",
                "french"],
            svec!["Familie Müller plant ihren Urlaub.
                Sie geht in ein Reisebüro und lässt sich von einem Angestellten beraten.
                Als Reiseziel wählt sie Mallorca aus.
                Familie Müller bucht einen Flug auf die Mittelmeerinsel.
                Sie bucht außerdem zwei Zimmer in einem großen Hotel direkt am Strand.
                Familie Müller badet gerne im Meer.
                Am Abflugtag fahren Herr und Frau Müller mit ihren beiden Kindern im Taxi zum Flughafen.
                Dort warten schon viele Urlauber. Alle wollen nach Mallorca fliegen.
                Familie Müller hat viel Gepäck dabei: drei große Koffer und zwei Taschen.
                Die Taschen sind Handgepäck. Familie Müller nimmt sie mit in das Flugzeug.
                Am Flugschalter checkt die Familie ein und erhält ihre Bordkarten.
                Die Angestellte am Flugschalter erklärt Herrn Müller den Weg zum Flugsteig.
                Es ist nicht mehr viel Zeit bis zum Abflug.
                Familie Müller geht durch die Sicherheitskontrolle.
                Als alle das richtige Gate erreichen, setzen sie sich in den Wartebereich.
                Kurz darauf wird ihre Flugnummer aufgerufen und Familie Müller steigt mit vielen anderen Passagieren in das Flugzeug nach Mallorca.
                Beim Starten fühlt sich Herr Müller nicht wohl.
                Ihm wird ein wenig übel.
                Nach zwei Stunden landet das Flugzeug.
                Am Gepäckband warten alle Passagiere noch auf ihr fehlendes Gepäck.
                Danach kann endlich der Urlaub beginnen.",
                "german"],
            svec!["У меня большая семья из шести человек: я, мама, папа, старшая сестра, бабушка и дедушка.
                Мы живем все вместе с собакой Бимом и кошкой Муркой в большом доме в деревне.
                Мой папа встает раньше всех, потому что ему рано на работу. Он работает доктором.
                Обычно бабушка готовит нам завтрак.
                Я обожаю овсяную кашу, а моя сестра Аня – блины.",
                "russian"],
        ];
        assert_eq!(got, expected);
    }

    #[test]
    fn lang_no_headers() {
        let wrk = Workdir::new("lang");
        wrk.create("data.csv", vec![
            svec!["Hello world"],
            svec!["Une phrase en français."],
            svec!["Familie Müller plant ihren Urlaub.
                Sie geht in ein Reisebüro und lässt sich von einem Angestellten beraten.
                Als Reiseziel wählt sie Mallorca aus.
                Familie Müller bucht einen Flug auf die Mittelmeerinsel.
                Sie bucht außerdem zwei Zimmer in einem großen Hotel direkt am Strand.
                Familie Müller badet gerne im Meer.
                Am Abflugtag fahren Herr und Frau Müller mit ihren beiden Kindern im Taxi zum Flughafen.
                Dort warten schon viele Urlauber. Alle wollen nach Mallorca fliegen.
                Familie Müller hat viel Gepäck dabei: drei große Koffer und zwei Taschen.
                Die Taschen sind Handgepäck. Familie Müller nimmt sie mit in das Flugzeug.
                Am Flugschalter checkt die Familie ein und erhält ihre Bordkarten.
                Die Angestellte am Flugschalter erklärt Herrn Müller den Weg zum Flugsteig.
                Es ist nicht mehr viel Zeit bis zum Abflug.
                Familie Müller geht durch die Sicherheitskontrolle.
                Als alle das richtige Gate erreichen, setzen sie sich in den Wartebereich.
                Kurz darauf wird ihre Flugnummer aufgerufen und Familie Müller steigt mit vielen anderen Passagieren in das Flugzeug nach Mallorca.
                Beim Starten fühlt sich Herr Müller nicht wohl.
                Ihm wird ein wenig übel.
                Nach zwei Stunden landet das Flugzeug.
                Am Gepäckband warten alle Passagiere noch auf ihr fehlendes Gepäck.
                Danach kann endlich der Urlaub beginnen."],
            svec!["У меня большая семья из шести человек: я, мама, папа, старшая сестра, бабушка и дедушка.
                Мы живем все вместе с собакой Бимом и кошкой Муркой в большом доме в деревне.
                Мой папа встает раньше всех, потому что ему рано на работу. Он работает доктором.
                Обычно бабушка готовит нам завтрак.
                Я обожаю овсяную кашу, а моя сестра Аня – блины."],
        ]);
        let mut cmd = wrk.command("lang");
        cmd.arg("1").arg("--no-headers").arg("data.csv");

        let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
        let expected = vec![
            svec!["Hello world",
                "english"],
            svec!["Une phrase en français.",
                "french"],
            svec!["Familie Müller plant ihren Urlaub.
                Sie geht in ein Reisebüro und lässt sich von einem Angestellten beraten.
                Als Reiseziel wählt sie Mallorca aus.
                Familie Müller bucht einen Flug auf die Mittelmeerinsel.
                Sie bucht außerdem zwei Zimmer in einem großen Hotel direkt am Strand.
                Familie Müller badet gerne im Meer.
                Am Abflugtag fahren Herr und Frau Müller mit ihren beiden Kindern im Taxi zum Flughafen.
                Dort warten schon viele Urlauber. Alle wollen nach Mallorca fliegen.
                Familie Müller hat viel Gepäck dabei: drei große Koffer und zwei Taschen.
                Die Taschen sind Handgepäck. Familie Müller nimmt sie mit in das Flugzeug.
                Am Flugschalter checkt die Familie ein und erhält ihre Bordkarten.
                Die Angestellte am Flugschalter erklärt Herrn Müller den Weg zum Flugsteig.
                Es ist nicht mehr viel Zeit bis zum Abflug.
                Familie Müller geht durch die Sicherheitskontrolle.
                Als alle das richtige Gate erreichen, setzen sie sich in den Wartebereich.
                Kurz darauf wird ihre Flugnummer aufgerufen und Familie Müller steigt mit vielen anderen Passagieren in das Flugzeug nach Mallorca.
                Beim Starten fühlt sich Herr Müller nicht wohl.
                Ihm wird ein wenig übel.
                Nach zwei Stunden landet das Flugzeug.
                Am Gepäckband warten alle Passagiere noch auf ihr fehlendes Gepäck.
                Danach kann endlich der Urlaub beginnen.",
                "german"],
            svec!["У меня большая семья из шести человек: я, мама, папа, старшая сестра, бабушка и дедушка.
                Мы живем все вместе с собакой Бимом и кошкой Муркой в большом доме в деревне.
                Мой папа встает раньше всех, потому что ему рано на работу. Он работает доктором.
                Обычно бабушка готовит нам завтрак.
                Я обожаю овсяную кашу, а моя сестра Аня – блины.",
                "russian"],
        ];
        assert_eq!(got, expected);
    }

    #[test]
    fn lang_column_name() {
        let wrk = Workdir::new("lang");
        wrk.create("data.csv", vec![
            svec!["text"],
            svec!["Hello world"],
            svec!["Une phrase en français."],
            svec!["Familie Müller plant ihren Urlaub.
                Sie geht in ein Reisebüro und lässt sich von einem Angestellten beraten.
                Als Reiseziel wählt sie Mallorca aus.
                Familie Müller bucht einen Flug auf die Mittelmeerinsel.
                Sie bucht außerdem zwei Zimmer in einem großen Hotel direkt am Strand.
                Familie Müller badet gerne im Meer.
                Am Abflugtag fahren Herr und Frau Müller mit ihren beiden Kindern im Taxi zum Flughafen.
                Dort warten schon viele Urlauber. Alle wollen nach Mallorca fliegen.
                Familie Müller hat viel Gepäck dabei: drei große Koffer und zwei Taschen.
                Die Taschen sind Handgepäck. Familie Müller nimmt sie mit in das Flugzeug.
                Am Flugschalter checkt die Familie ein und erhält ihre Bordkarten.
                Die Angestellte am Flugschalter erklärt Herrn Müller den Weg zum Flugsteig.
                Es ist nicht mehr viel Zeit bis zum Abflug.
                Familie Müller geht durch die Sicherheitskontrolle.
                Als alle das richtige Gate erreichen, setzen sie sich in den Wartebereich.
                Kurz darauf wird ihre Flugnummer aufgerufen und Familie Müller steigt mit vielen anderen Passagieren in das Flugzeug nach Mallorca.
                Beim Starten fühlt sich Herr Müller nicht wohl.
                Ihm wird ein wenig übel.
                Nach zwei Stunden landet das Flugzeug.
                Am Gepäckband warten alle Passagiere noch auf ihr fehlendes Gepäck.
                Danach kann endlich der Urlaub beginnen."],
            svec!["У меня большая семья из шести человек: я, мама, папа, старшая сестра, бабушка и дедушка.
                Мы живем все вместе с собакой Бимом и кошкой Муркой в большом доме в деревне.
                Мой папа встает раньше всех, потому что ему рано на работу. Он работает доктором.
                Обычно бабушка готовит нам завтрак.
                Я обожаю овсяную кашу, а моя сестра Аня – блины."],
        ]);
        let mut cmd = wrk.command("lang");
        cmd.arg("text").arg("-c").arg("language").arg("data.csv");

        let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
        let expected = vec![
            svec!["text",
                "language"],
            svec!["Hello world",
                "english"],
            svec!["Une phrase en français.",
                "french"],
            svec!["Familie Müller plant ihren Urlaub.
                Sie geht in ein Reisebüro und lässt sich von einem Angestellten beraten.
                Als Reiseziel wählt sie Mallorca aus.
                Familie Müller bucht einen Flug auf die Mittelmeerinsel.
                Sie bucht außerdem zwei Zimmer in einem großen Hotel direkt am Strand.
                Familie Müller badet gerne im Meer.
                Am Abflugtag fahren Herr und Frau Müller mit ihren beiden Kindern im Taxi zum Flughafen.
                Dort warten schon viele Urlauber. Alle wollen nach Mallorca fliegen.
                Familie Müller hat viel Gepäck dabei: drei große Koffer und zwei Taschen.
                Die Taschen sind Handgepäck. Familie Müller nimmt sie mit in das Flugzeug.
                Am Flugschalter checkt die Familie ein und erhält ihre Bordkarten.
                Die Angestellte am Flugschalter erklärt Herrn Müller den Weg zum Flugsteig.
                Es ist nicht mehr viel Zeit bis zum Abflug.
                Familie Müller geht durch die Sicherheitskontrolle.
                Als alle das richtige Gate erreichen, setzen sie sich in den Wartebereich.
                Kurz darauf wird ihre Flugnummer aufgerufen und Familie Müller steigt mit vielen anderen Passagieren in das Flugzeug nach Mallorca.
                Beim Starten fühlt sich Herr Müller nicht wohl.
                Ihm wird ein wenig übel.
                Nach zwei Stunden landet das Flugzeug.
                Am Gepäckband warten alle Passagiere noch auf ihr fehlendes Gepäck.
                Danach kann endlich der Urlaub beginnen.",
                "german"],
            svec!["У меня большая семья из шести человек: я, мама, папа, старшая сестра, бабушка и дедушка.
                Мы живем все вместе с собакой Бимом и кошкой Муркой в большом доме в деревне.
                Мой папа встает раньше всех, потому что ему рано на работу. Он работает доктором.
                Обычно бабушка готовит нам завтрак.
                Я обожаю овсяную кашу, а моя сестра Аня – блины.",
                "russian"],
        ];
        assert_eq!(got, expected);
    }
}
