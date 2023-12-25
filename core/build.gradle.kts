dependencies {
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")

    api(platform("io.arrow-kt:arrow-stack:1.2.0"))
    api("io.arrow-kt:arrow-core")

    testImplementation("io.arrow-kt:arrow-fx-coroutines")
}
